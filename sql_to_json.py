import sqlite3
import json
import ast
import pandas
import os
import ntpath

class SqlToJson:

    def __init__(self):
        self.pp_count = 1
        self.pd_count = 1
        self.dp_count = 1

    def get_info_from_sql(self, input_db_file, run_num):
        """ queries noWorkflow sql database """

        db = sqlite3.connect(input_db_file, uri=True)
        c = db.cursor()

        # script_name
        '''
        c.execute('SELECT id, command from trial where id = ?', (run_num, ))
        temp = c.fetchone()
        temp = temp[1]
        temp = temp.split(" ")
        script_name = temp[3]
        '''

        c.execute("select name, value from environment_attr where trial_id = ?", (run_num,))
        environmentInfo = c.fetchall()
        self.envir = {}
        for envVal in environmentInfo:
            self.envir[envVal[0]] = envVal[1]

        c.execute("select trial_id, id, name, value, line from variable where variable.type = 'normal' and trial_id = ?", (run_num,))
        self.var_info = c.fetchall()
        self.var_info = pandas.DataFrame(self.var_info)
        self.var_info.columns = ["trial_id", "id", "name", "value", "line"]
        # process nodes
        c.execute('SELECT trial_id, id, name, return_value, line from function_activation where trial_id = ?', (run_num,))
        script_steps = c.fetchall()
        c.execute("with funcLines as (SELECT line from function_activation where trial_id = ?) select trial_id, id, name, value, line from variable where variable.type = 'normal' and variable.line not in funcLines", (run_num,))
        self.data_assign = c.fetchall()
        script_name = ntpath.basename(script_steps[0][2])
        self.procNodes = []
        '''
        for i, step in enumerate(script_steps):
            if(i == 0):
                step = list(step)
                step[4] = 0
            self.procNodes.append(list(step))
        '''
        for i, assignment in enumerate(script_steps):
            if(i == 0 ):
                continue
            thisLine = None
            script_steps[i] = list(assignment)
            with open(script_name) as f:
                # subtract 1 from s[4] because script_steps starts at [1] to avoid redundant start node
                for j, line in enumerate(f):
                    if (j + 1) == assignment[4]:
                        thisLine = line
                    elif (j + 1) > assignment[4]:
                        break
            script_steps[i].append(thisLine)
            self.procNodes.append(script_steps[i])
        
        for i, assignment in enumerate(self.data_assign):
            thisLine = None
            self.data_assign[i] = list(assignment)
            with open(script_name) as f:
                # subtract 1 from s[4] because script_steps starts at [1] to avoid redundant start node
                for j, line in enumerate(f):
                    if (j + 1) == assignment[4]:
                        thisLine = line
                    elif (j + 1) > assignment[4]:
                        break
            self.data_assign[i].append(thisLine)
            self.procNodes.append(self.data_assign[i])

        self.procNodes = pandas.DataFrame(self.procNodes)
        self.procNodes.columns = ["trial_id", "id", "name", "value", "line", "code"]
        self.procNodes = self.procNodes.sort_values("line")
        self.procNodes.index = range(len(self.procNodes.index))
        print(self.procNodes)

        #trial_id, id, name, return_value, line

        # file io nodes
        c.execute('SELECT trial_id, name, function_activation_id, mode, content_hash_after from file_access where trial_id = ?' , (run_num, ))
        files = c.fetchall()

        # dict for easier access to file info
        temp = {}
        for f in files:
            d = {"name": f[1], "mode": f[3], "hash" : f[4]}
            temp[f[2]] = d
        files = temp

        # functions
        c.execute('SELECT name, trial_id, last_line from function_def where trial_id = ?', (run_num, ))
        func_ends = c.fetchall()

        # dict for easier access to func_ends. used for collapsing nodes
        temp = {}
        end_funcs = {}
        for f in func_ends:
            temp[f[0]] = f[2]
            end_funcs[f[2]] = f[0]
        func_ends = temp

        # if f has return value, f[2]-=1
        # so last line detected correctly
        # last line informs the finish node + allows for sequential functions
        for f in func_ends:
            c.execute('SELECT trial_id, name, return_value from function_activation where trial_id = ? and name = ?', (run_num, f, ))
            calls = c.fetchall()
            for call in calls:
                if call[2]!="None":
                    func_ends[f]-=1
                    temp = func_ends[f]
                    end_funcs[temp]=f

        c.close()

        return script_steps, files, func_ends, end_funcs, script_name

    def get_defaults(self, script_name):
        """ sets default required fields for the Prov-JSON file, ie environment node
        variable 'rdt:script' is the first script in the workflow.
        """
        result, activity_d, environment_d = {}, {}, {}
        environment_d["rdt:name"] = "environment"
        environment_d["rdt:architecture"] = self.envir["ARCH"]
        environment_d["rdt:operatingSystem"] = self.envir["OS_NAME"] + " " + self.envir["OS_VERSION"]
        environment_d["rdt:langVersion"] = self.envir["PYTHON_VERSION"]
        environment_d["rdt:workingDirectory"] = self.envir["PWD"]
        environment_d['rdt:language'] = "Python"
        environment_d["rdt:script"] = script_name
        #activity_d['environment'] = environment_d

        prefix, agent = {}, {}
        prefix["prov"] = "http://www.w3.org/ns/prov#"
        prefix["rdt"] = "http://rdatatracker.org/"

        result['activity']= activity_d
        result["prefix"] = prefix

        a1 = {}
        a1["rdt:tool.name"] = "noWorkflow"
        a1["rdt:tool.version"] = self.envir["NOWORKFLOW_VERSION"]
        a1["rdt:json.version"] = "2.1"
        agent["a1"] = a1

        result["agent"] = agent

        keys = ["entity", "wasInformedBy", "wasGeneratedBy", "used"]
        for i in range (0, len(keys)):
            result[keys[i]]={}
        result["entity"]["environment"] = environment_d

        return result

    def add_informs_edge(self, result, prev_p, current_p, e_count):
        """ adds informs edge between steps in the script or between script nodes """

        current_informs_edge = {}
        current_informs_edge['prov:informant'] = prev_p
        current_informs_edge['prov:informed'] = current_p

        ekey_string = "pp" + str(self.pp_count)
        self.pp_count += 1

        result['wasInformedBy'][ekey_string] = current_informs_edge

        return e_count

    def add_start_node(self, result, step, p_count, next_line=None):
        """ adds start node and edge for current step """

        # make node
        start_node_d = {}
        start_node_d['rdt:type'] = "Start"
        start_node_d["rdt:elapsedTime"] = "0.5"
        keys = ["rdt:scriptNum", "rdt:startLine", "rdt:startCol", "rdt:endLine", "rdt:endCol"]
        for key in keys:
            start_node_d[key] = "NA"

        # choose most descriptive label for the node
        if next_line:
            start_node_d['rdt:name'] = next_line
        else:
            start_node_d['rdt:name'] = step[2]

        pkey_string = "p" + str(p_count)
        prev_p = pkey_string
        p_count+=1

        # add node
        result['activity'][pkey_string] = start_node_d

        return prev_p, p_count

    def add_end_node(self, result, p_count, name):
        """ makes Finish node so that the function or loop is collapsible """

        # make node
        end_node_d = {}
        end_node_d['rdt:name'] = name
        end_node_d['rdt:type'] = "Finish"
        end_node_d["rdt:elapsedTime"] = "0.5"
        keys = ["rdt:scriptNum", "rdt:startLine", "rdt:startCol", "rdt:endLine", "rdt:endCol"]
        for key in keys:
            end_node_d[key] = "NA"

        # add node
        pkey_string = "p" + str(p_count)
        p_count += 1
        result['activity'][pkey_string] = end_node_d

        return pkey_string, p_count

    def add_process(self, result, p_name, p_count, s, script_name, next_line):
        """ adds process node and edge for each step in script_steps
        chooses the most descriptive label for the node between:
        noWorkflow default step label or the relevent line in the script"""

        # defaults for all process nodes
        current_process_node = {}
        current_process_node['rdt:type'] = "Operation"
        current_process_node["rdt:elapsedTime"] = "TODO"
        current_process_node["rdt:startLine"], current_process_node["rdt:endLine"] = str(s[4]), str(s[4])

        # get most descriptive label and dependent properties
        if s[2].startswith("__") or s[2]=="f" and next_line != "":
            line_label = next_line.strip()
        else:
            line_label = s[5]

        current_process_node['rdt:name'] = line_label
        current_process_node["rdt:startCol"] = str(0)
        current_process_node["rdt:endCol"] = str(len(line_label))
        current_process_node["rdt:scriptNum"] = str(0)

        # add the node
        pkey_string = "p" + str(p_count)
        p_count += 1
        result["activity"][pkey_string] = current_process_node

        return p_count, pkey_string

    def add_file_node(self, script, current_link_dict, d_count, result, data_dict):
        """ adds a file node, called by add_file """

        #make file node
        current_file_node = {}
        current_file_node['rdt:name'] = script
        current_file_node['rdt:type'] = "File"
        keys = ['rdt:scope', "rdt:fromEnv", "rdt:timestamp", "rdt:location"]
        values = ["undefined", "FALSE", "", ""]
        for i in range (0, len(keys)):
            current_file_node[keys[i]] = values[i]

        split_path_file = current_link_dict['name'].split("/")

        # set value/relative path according to file's parent directory
        try:
            if split_path_file[1] == "results":
                current_file_node['rdt:value'] = script # if result/in results dir
            elif split_path_file[1] == "data":
                current_file_node['rdt:value'] = "." + current_link_dict['name']
            else:
                # if not in data or results, put entire path
                current_file_node['rdt:value']= current_link_dict['name']

        # avoid errors if the file name is not a full path and put entire path
        except:
            current_file_node['rdt:value']= current_link_dict['name']

        # add file node
        dkey_string = "d" + str(d_count)
        d_count+=1
        result["entity"][dkey_string] = current_file_node

        # add to dict of edges to make connections b/w graphs
        data_dict[script] = dkey_string

        return d_count, dkey_string

    def add_file_edge(self, current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles):
        """ adds a file edge, called by add_file """

        # make edge
        current_edge_node = {}
        current_edge_node['prov:activity'] = current_p
        current_edge_node['prov:entity'] = dkey_string

        # add edge
        e_string = "e" + str(e_count)
        e_count+=1

        if current_link_dict['mode'] == "r":
            result['used'][e_string] = current_edge_node
        else:
            result['wasGeneratedBy'][e_string] = current_edge_node
            # if file created, add to outfiles dict for linking graphs
            inner_dict = {'data_node_num': dkey_string, 'source': activation_id_to_p_string[s[1]], 'hash_out': h}
            outer_dict = {path_array[-1] : inner_dict}
            outfiles[first_step[2]] = outer_dict

    def add_file(self, result, files, d_count, e_count, current_p, s, outfiles, first_step, activation_id_to_p_string, data_dict):
        """ uses files dict to add file nodes and access edges to the dictionary
        uses outfiles dict to check if file already exists from a previous script """

        dkey_string = -1

        # get file_name
        current_link_dict = files[s[1]]
        path_array = current_link_dict['name'].split("/")

        #get hash
        file_entry = files[s[1]]
        h = file_entry['hash']

        if len(outfiles.keys()) !=0:
        # if not first script, check to see if the file already has a node using name and hash
            for script in outfiles.keys():
                    for outfile in outfiles[script]:
                        # if already seen
                        if outfile == path_array[-1] and outfiles[script][outfile]['hash_out']==h:
                            # do not add new node, but return d_key_string
                            dkey_string = data_dict[path_array[-1]]

            if dkey_string == -1: # if not seen yet, add node
                d_count, dkey_string = self.add_file_node(path_array[-1], current_link_dict, d_count, result, data_dict)
            # add new dependent edge
            self.add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles)

        # if first script, add file nodes w/o checking prior existence
        else:
            d_count, dkey_string = self.add_file_node(path_array[-1], current_link_dict, d_count, result, data_dict)
            self.add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles)

        return d_count, e_count

    def add_data_edge(self, result, s, d_count, e_count, current_p, script_name):
        """ makes intermediate data node if process had return value
        if dataframe, make a snapshot csv
        else, make a normal data node as a string """
        varName = self.var_info[self.var_info["line"] == s[4]]["name"].values
        if(len(varName) > 0):
            varName = varName[0]
        else:
            varName = "data"
        # make data node
        current_data_node = {}
        current_data_node['rdt:name'] = varName
        current_data_node['rdt:scope'] = "Global"
        keys = ["rdt:fromEnv", "rdt:timestamp", "rdt:location"]
        values = ["FALSE", "", ""]
        for i in range (0, len(keys)):
            current_data_node[keys[i]] = values[i]

        # -------STRING FORMATTING FOR PRINTING---------
        # Cases to test:
        # full df
        # full labeled df
        # labelled subset
        # unlabelled subset
        # labelled subset len >1
        # unlabelled subset len >1
        # Check all if statements with more script examples

        df = None

        if s[3]!= None:
            y = s[3].split("\n")

            # use first line and last line to figure out formatting
            first_line = y[0].strip()
            last_line = y[-1].strip()

            # convert to df if full or subsetted df

            if "Unnamed:" in first_line: # entire dataframe
                col_names = first_line.split()[1:]
                data = []
                for l in y[1:]:
                    line = l.split()[1:]
                    data.append(line)
                df = pandas.DataFrame(data, columns = col_names)

            elif "Name:" in last_line: # subset of dataframe
                col_names = []
                temp = last_line.split()
                for i in range (1, int(len(temp)/2), 2):
                    col_names.append(temp[i].strip(","))
                data = []
                for l in y[:-1]:
                    line = l.split()[1:]
                    data.append(line)
                df = pandas.DataFrame(data, columns = col_names)

            else: # integer value or different type of return statement
                # debug or return as string
                print("something else, need to debug or just return as a string")

        if isinstance(df, pandas.core.frame.DataFrame):
            current_data_node['rdt:type'] = "Snapshot"
            # make dir if it doesn't exist
            filename = "line" + str(s[4]) + "data.csv"
            script = script_name.split("/")[-1].strip(".py")
            # TO DO: relative path
            directory = "/Users/jen/Desktop/newNow/data/intermediate_values_of_" + script + "_data/"
            if not os.path.exists(directory):
                os.makedirs(directory)
            path = directory + filename
            #write to csv
            df.to_csv(path)
            # TO DO: relative path
            # current_data_node['rdt:value'] = path
            current_data_node['rdt:value'] = "../data/intermediate_values_of_testJ_data/" + filename
        else:
            current_data_node['rdt:type'] = "Data"
            if s[3]!=None:
                current_data_node['rdt:value'] = s[3]
            else:
                current_data_node['rdt:value'] = "None"
        if(current_data_node['rdt:value'] != "None"):
            # add data node
            dkey_string = "d" + str(d_count)
            d_count+=1
            result["entity"][dkey_string] = current_data_node

            # make edge
            current_edge_node = {}
            current_edge_node['prov:activity'] = current_p
            current_edge_node['prov:entity'] = dkey_string

            # add edge
            e_string = "pd" + str(self.pd_count)
            self.pd_count+=1
            result['wasGeneratedBy'][e_string] = current_edge_node
        else:
            dkey_string = None

        return d_count, e_count, dkey_string

    def get_arguments_from_sql(self, input_db_file, return_value, run_num, activation_id_to_p_string):
        """ queries sql database to find functions dependent on intermediate return value
        returns the process_string of these processes """

        target_processes = []
        db = sqlite3.connect(input_db_file, uri=True)
        c = db.cursor()

        c.execute('SELECT trial_id, value, function_activation_id from object_value where trial_id = ? and value = ?', (run_num, return_value, ))
        all_dep_processes = c.fetchall()

        # get all dependent processes and convert to p_string
        for p in all_dep_processes:
            process = p[2]
            p_string = activation_id_to_p_string[process]
            target_processes.append(p_string)

        return target_processes

    def int_data_to_process(self, dkey_string, process_string, e_count, result):
        """ adds edge from intermediate data node to dependent process node """

        # make edge
        current_edge_node = {}
        current_edge_node['prov:activity'] = process_string
        current_edge_node['prov:entity'] = dkey_string

        # add edge
        e_string = "dp" + str(self.dp_count)
        self.dp_count+=1
        result['used'][e_string] = current_edge_node

        return e_count

    def make_dict(self, script_steps, files, input_db_file, run_num, func_ends, end_funcs, p_count, d_count, e_count, outfiles, result, data_dict, finish_node, script_name, loop_dict):
        """ uses the information from the database
        to make a dictionary compatible with Prov-JSON format

        1. Get Defaults and start node
        2. Loop through script_steps
            a. Make process nodes
            b. Check and add file acccesses and edges
            c. Check and add intermediate data values and edges
            d. Make informs edges
        3. Make finish node and final informs edge
        """

        # if first script in list, set up the default formats
        if len(result.keys()) == 0:
            result = self.get_defaults(script_name)

        # if not first script, add informs edge between
        # the Finish of the previous script and the Start of the current script
        if finish_node!= None:
            current_p = "p" + str(p_count)
            e_count = self.add_informs_edge(result, finish_node, current_p, e_count)

        # initialize per-script variables
        process_stack, loop_name_stack, loop_stack, function_stack = [], [], [], []
        int_values, int_dkey_strings = [], []
        dkey_string = -1
        activation_id_to_p_string = {}

        prev_p, p_count = self.add_start_node(result, script_steps[0], p_count)
        process_stack.append(script_steps[0][4])
        function_stack.append(script_steps[0][4])
        current_line = ""

        

        # iterate through each line in the script
        for i in range (0, len(self.procNodes.index)):
            #s = script_steps[i]
            s = tuple(self.procNodes.iloc[i].values)

            # get the line of the script
            next_line=""
            with open(script_name) as f:
                # subtract 1 from s[4] because script_steps starts at [1] to avoid redundant start node
                for j, line in enumerate(f):
                    if j == s[4]-1:
                        next_line = line
                    elif j > s[4]-1:
                        break

            # if loop has ended on current step, add finish node
            #if len(loop_stack)>0 and s[4] >= loop_stack[-1]:
            if False:
                # get the function name
                func_name = loop_name_stack.pop()

                # add the finish node and pop from the stacks
                current_p, p_count = self.add_end_node(result, p_count, func_name)
                process_stack.pop()
                loop_stack.pop()

                # add informs edge between last process node in loop and the finish node of the loop
                e_count = self.add_informs_edge(result, prev_p, current_p, e_count)
                prev_p = "p" + str(p_count-1)

            # if current step is a function, add start node
            # store the function_activation_id in stack to be able to make Finish node
            #if s[2] in func_ends:
            if False:
                current_p, p_count = self.add_start_node(result, s, p_count)
                process_stack.append(func_ends[s[2]])
                function_stack.append(func_ends[s[2]])

            # if current_step is the start of a loop, add start node
            # store the last line in loop in stack to be able to make Finish node
            #elif s[4] in loop_dict.keys():
            if False:
                current_p, p_count = self.add_start_node(result, s, p_count, next_line.strip())
                process_stack.append(loop_dict[s[4]])
                loop_name_stack.append(next_line.strip())
                loop_stack.append(loop_dict[s[4]])

            # if no special cases, add normal process node
            else:
                p_count, current_p = self.add_process(result, s[2], p_count, s, script_name, next_line)

            # dict for use in get_arguments_from_sql
            activation_id_to_p_string[s[1]] = current_p

            # if process node reads or writes to file, add file nodes and edges
            # TO DO: read file not detected unless with open() as f format.
            if s[1] in files.keys():
                d_count, e_count = self.add_file(result, files, d_count, e_count, current_p, s, outfiles, script_steps[0], activation_id_to_p_string, data_dict)

            # if process node has return statement, make intermediate data node and edges
            if s[3] != "None":
                d_count, e_count, dkey_string = self.add_data_edge(result, s, d_count, e_count, current_p, script_name)
                int_values.append(s[3])
                int_dkey_strings.append(dkey_string)
            int_dkey_strings = [x for x in int_dkey_strings if x is not None]

            # add_informs_edge between all process nodes
            e_count = self.add_informs_edge(result, prev_p, current_p, e_count)
            prev_p = "p" + str(p_count-1)

            # if function, NOT LOOP, has ended on current step, add finish node
            if s[4] == function_stack[-1]:
                # get the function name
                func_name = end_funcs[s[4]]

                # add the finish node and pop from the stack
                current_p, p_count = self.add_end_node(result, p_count, func_name)
                process_stack.pop()
                function_stack.pop()

                # add informs edge between last process node in loop and the finish node of the loop
                e_count = self.add_informs_edge(result, prev_p, current_p, e_count)
                prev_p = "p" + str(p_count-1)

        # after all steps in script done
        # add finish nodes (both loops and functions)
        #and informs edges for the rest of the process_stack
        while len(process_stack)>1:
            func_line = process_stack.pop()
            try: # get the func name
                func_name = end_funcs[func_line]
            except: # get the loop name
                func_name = loop_name_stack.pop()

            # add the finish node and edge
            current_p, p_count = self.add_end_node(result, p_count, func_name)
            e_count = self.add_informs_edge(result, prev_p, current_p, e_count)
            prev_p = "p" + str(p_count-1)

        # add finish node and final informs edge for the script
        current_p, p_count = self.add_end_node(result, p_count, script_steps[0][2])
        e_count = self.add_informs_edge(result, prev_p, current_p, e_count)

        # adds used edges using dependencies from database table: object_value
        # TO DO: prevent edges that go up?
        for i in range (0, len(int_values)):
            # print(i) #52309
            return_value = int_values[i]
            target_processes = self.get_arguments_from_sql(input_db_file, return_value, run_num, activation_id_to_p_string)
            for process in target_processes:
                e_count = self.int_data_to_process(int_dkey_strings[i], process, e_count, result)

        return result, p_count, d_count, e_count, outfiles, current_p

    def get_loop_locations(self, script_name):
        """ uses ast module to find the start and end lines of for and while loops
        to allow for collapsible nodes for loops (as well as functions) """

        loop_dict = {}

        with open(script_name) as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, (ast.For, ast.While)):
                # keys = start line, values = finish line
                # offset by 1 to match with script_steps numbering
                loop_dict[node.lineno] = node.body[-1].lineno+1

        return loop_dict

    def write_json(self, dictionary, output_json_file):
        with open(output_json_file, 'w') as outfile:
            json.dump(dictionary, outfile, default=lambda temp: json.loads(temp.to_json()))

    def link_DDGs(self, trial_num_list, input_db_file, output_json_file):
        """ input: db_file generated by noworkflow
        target path where the Prov-JSON file will be written
        and a list of trial numbers that will be linked together into a DDG
        where trial numbers correspond to individual scripts stored in the noworkflow database

        output: prov-json file that can be opened in DDG Explorer
        """

        # initialize variables that will carry over from 1 script to the next
        p_count, d_count, e_count = 1, 1, 1
        result, outfiles, data_dict = {}, {}, {}
        finish_node = None

        # for each trial, query and add to the result
        for trial_num in trial_num_list:
            script_steps, files, func_ends, end_funcs, script_name = self.get_info_from_sql(input_db_file, trial_num)
            loop_dict = self.get_loop_locations(script_name)
            result, p_count, d_count, e_count, outfiles, finish_node = self.make_dict(script_steps, files, input_db_file, trial_num, func_ends, end_funcs, p_count, d_count, e_count, outfiles, result, data_dict, finish_node, script_name, loop_dict)

        # Write to file
        self.write_json(result, output_json_file)

    def convert(self, trialNumList, inputDB, outputJSON):
        self.link_DDGs(trialNumList, inputDB, outputJSON)

    '''
    def main():

        # TO DO: how to get these paths?
        input_db_file = '/Users/jen/Desktop/newNow/scripts/.noworkflow/db.sqlite'
        output_json_file = "/Users/jen/Desktop/newNow/results/J.json"

        # TO DO: how to get these from now list, how to make sure they are in order?
        trial_num_list = [13]

        link_DDGs(trial_num_list, input_db_file, output_json_file)

        # TO DO: how to open DDG Explorer automatically?

    if __name__ == "__main__":
        main()
    '''