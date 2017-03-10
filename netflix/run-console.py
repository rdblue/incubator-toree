#!/usr/bin/env python


import os, sys


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

# from Toree's install script
def find_py4j(spark_home):
    python_lib_contents = os.listdir("{0}/python/lib".format(spark_home))
    try:
        return list(filter(lambda filename: "py4j" in filename, python_lib_contents))[0]
    except:
        return None

def mkdir_p(dir_path, mode=0777):
    if not os.path.exists(dir_path):
        mkdir_p(os.path.dirname(dir_path))
        if not os.path.exists(dir_path):
            os.mkdir(dir_path, mode)

def main(args):
    java_options, spark_args = get_value(args, '--driver-java-options')
    driver_java_options = " ".join([ jarg for jarg in ["-noverify", java_options] if jarg ])

    work_path = os.getenv('CURRENT_JOB_WORKING_DIR')

    # the connection file path needs to be passed to both jupyter console and toree
    connect_file_path = os.path.join(work_path, 'connect.json')

    # log outside of the work path to avoid logs disappearing when the kernel restarts
    log_path = os.path.join(os.path.dirname(work_path), os.path.basename(work_path) + '.log')
    spark_home = os.getenv('SPARK_HOME')
    spark_env_opts = os.getenv('SPARK_OPTS')
    toree_assembly = os.getenv('TOREE_ASSEMBLY')
    toree_env_opts = os.getenv('TOREE_OPTS')

    if not spark_home:
        raise StandardError("SPARK_HOME is not set")

    if not toree_assembly:
        raise StandardError("TOREE_ASSEMBLY is not set")

    if not work_path:
        raise StandardError("CURRENT_JOB_WORKING_DIR is not set")

    # dsespark-submit will remove the first arg, scala-kernel
    kernel_cmd_args = ["{spark}/bin/dsespark-submit.py".format(spark=spark_home), 'scala-kernel']

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

    # add toree args
    kernel_cmd_args.append("--class")
    kernel_cmd_args.append("org.apache.toree.Main")
    kernel_cmd_args.append(toree_assembly)
    kernel_cmd_args.extend(toree_env_opts.split() if toree_env_opts else [])
    kernel_cmd_args.append("--profile")
    kernel_cmd_args.append(connect_file_path)

    # the Toree command is run by jupyter console. it is passed in as a python list.
    kernel_cmd = repr(kernel_cmd_args)

    sys.stderr.write("\nSpark application log path: " + log_path + "\n\n")

    py4j = find_py4j(spark_home)
    if py4j:
        os.environ['PYTHON_PATH'] = "{spark_home}/python:{spark_home}/python/lib/{py4j}".format(spark_home=spark_home, py4j=py4j)

    command = 'jupyter'

    command_args = ['console']
    command_args.append('--KernelManager.kernel_cmd=' + kernel_cmd)
    command_args.append('--ZMQTerminalIPythonApp.connection_file=' + connect_file_path)

    os.execv(command, command_args)

if __name__ == '__main__':
    # remove the command name
    main(sys.argv[2:])
