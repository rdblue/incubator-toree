#!/usr/bin/env python


import os, sys

TOREE_OPTION_KEYS = {
        '--idle-timeout': 1,
        '--spark-idle-timeout': 1,
        '--default-interpreter': 1
    }

def split_args(args):
    """Partitions the argument list using a map of known options
    """
    spark_args = []
    toree_args = []

    i = 0
    while i < len(args):
        if args[i] in TOREE_OPTION_KEYS.keys():
            toree_args.append(args[i])
            num_values = TOREE_OPTION_KEYS[args[i]]
            # consume the arg's values
            for _ in range(num_values):
                i += 1
                toree_args.append(args[i])
        else:
            spark_args.append(args[i])

        i += 1

    return (spark_args, toree_args)

def get_value(args, arg_key):
    """Returns an argument's value from the list of args
    """
    remaining = []
    value = None
    i = 0
    while i < len(args):
        if arg_key == args[i]:
            i += 1
            value = args[i]
        else:
            remaining.append(args[i])

        i += 1

    return (value, remaining)

def find_py4j(spark_home):
    # Find out where our matching py4j is located
    zips = glob.glob(os.path.join(spark_home, 'python', 'lib', 'py4j-*.zip'))
    if zips:
        return zips[0]
    return None

def mkdir_p(dir_path, mode=0777):
    if not os.path.exists(dir_path):
        mkdir_p(os.path.dirname(dir_path))
        if not os.path.exists(dir_path):
            os.mkdir(dir_path, mode)

def main(script_name, args):
    spark_args, toree_args = split_args(args)
    java_options, spark_args = get_value(spark_args, '--driver-java-options')
    # get --jars, split on comma, and filter empty values
    jarArg, spark_args = get_value(spark_args, '--jars')
    jars = [ jar for jar in jarArg.split(',') if jar ] if jarArg else []
    driver_java_options = " ".join([ jarg for jarg in ["-noverify", java_options] if jarg ])

    spark_home = os.getenv('SPARK_HOME')
    spark_env_opts = os.getenv('SPARK_OPTS')
    toree_assembly = os.getenv('TOREE_ASSEMBLY')
    toree_env_opts = os.getenv('TOREE_OPTS')
    work_path = os.getenv('CURRENT_JOB_WORKING_DIR')

    if not spark_home:
        raise StandardError("SPARK_HOME is not set")

    if not toree_assembly:
        raise StandardError("TOREE_ASSEMBLY is not set")

    jars.insert(0, toree_assembly)

    if not work_path:
        raise StandardError("CURRENT_JOB_WORKING_DIR is not set")

    # the connection file path needs to be passed to both jupyter console and toree
    connect_file_path = os.path.join(work_path, 'connect.json')

    # log outside of the work path to avoid logs disappearing when the kernel restarts
    log_path = os.path.join(os.path.dirname(work_path), os.path.basename(work_path) + '.log')

    # dsespark-submit will remove the first arg, scala-kernel
    kernel_cmd_args = ["{spark}/bin/dsespark-submit.py".format(spark=spark_home)]

    # add the user's spark properties, if present. this comes before spark CLI
    # arguments so that properties set on the command line take precedence.
    extra_properties_path = os.path.expanduser('~/.spark.properties')
    if os.path.exists(extra_properties_path):
        kernel_cmd_args.append('--extra-properties-file')
        kernel_cmd_args.append(extra_properties_path)
    else:
        extra_properties_path = os.path.expanduser('~/notebooks/spark.properties')
        if os.path.exists(extra_properties_path):
            kernel_cmd_args.append('--extra-properties-file')
            kernel_cmd_args.append(extra_properties_path)

    # add spark args
    kernel_cmd_args.extend(spark_env_opts.split() if spark_env_opts else [])
    kernel_cmd_args.extend(spark_args)
    kernel_cmd_args.append("--conf")
    kernel_cmd_args.append("spark.log.path=" + log_path)
    kernel_cmd_args.append("--driver-java-options")
    kernel_cmd_args.append(driver_java_options)
    kernel_cmd_args.append("--deploy-mode")
    kernel_cmd_args.append("client")
    kernel_cmd_args.append("--jars")
    kernel_cmd_args.append(','.join(jars))

    kernel_cmd_args.append("--class")
    kernel_cmd_args.append("org.apache.toree.Main")
    kernel_cmd_args.append(toree_assembly)

    # add toree args
    interpreter, toree_args = get_value(toree_args, '--default-interpreter')
    if not interpreter and script_name == 'sql-console':
        interpreter = 'SQL'

    kernel_cmd_args.extend(toree_env_opts.split() if toree_env_opts else [])
    if interpreter:
        kernel_cmd_args.append('--default-interpreter')
        kernel_cmd_args.append(interpreter)
    kernel_cmd_args.extend(toree_args)
    kernel_cmd_args.append("--profile")
    kernel_cmd_args.append(connect_file_path)

    # the Toree command is run by jupyter console. it is passed in as a python list.
    kernel_cmd = '[' + ','.join([ repr(arg) for arg in kernel_cmd_args ]) + ']'

    sys.stderr.write("\nSpark application log path: " + log_path + "\n\n")

    py4j = find_py4j(spark_home)
    if py4j:
        os.environ['PYTHONPATH'] = os.pathsep.join([os.path.join(spark_home, 'python'), py4j])

    command = '/usr/local/bin/jupyter'

    command_args = ['', 'console']
    command_args.append('--KernelManager.kernel_cmd=' + kernel_cmd)
    command_args.append('--ZMQTerminalIPythonApp.connection_file=' + connect_file_path)

    os.execv(command, command_args)

if __name__ == '__main__':
    # remove the command name
    main(sys.argv[1], sys.argv[2:])
