#!/usr/bin/env python


import os, sys

TOREE_OPTION_KEYS = {
        '--profile': 1,
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
    spark_args, toree_args = split_args(args)
    java_options, spark_args = get_value(spark_args, '--driver-java-options')
    driver_java_options = " ".join([ jarg for jarg in ["-noverify", java_options] if jarg ])

    work_path = os.getenv('CURRENT_JOB_WORKING_DIR')
    # log to /var/log/spark to avoid logs disappearing when the kernel restarts
    mkdir_p('/var/log/spark')
    log_path = os.path.join('/var/log/spark', os.path.basename(work_path) + '.log')
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

    command = "{spark}/bin/dsespark-submit.py".format(spark=spark_home)

    # dsespark-submit will remove the first arg
    command_args = ['scala-kernel']

    # add the user's spark properties, if present. this comes before spark CLI
    # arguments so that properties set on the command line take precedence.
    extra_properties_path = os.path.expanduser('~/.spark.properties')
    if os.path.exists(extra_properties_path):
        command_args.append('--extra-properties-file')
        command_args.append(extra_properties_path)

    # add spark args
    command_args.extend(spark_env_opts.split() if spark_env_opts else [])
    command_args.extend(spark_args)
    command_args.append("--conf")
    command_args.append("spark.log.path=" + log_path)
    command_args.append("--driver-java-options")
    command_args.append(driver_java_options)
    command_args.append("--deploy-mode")
    command_args.append("client")

    # add toree args
    command_args.append("--class")
    command_args.append("org.apache.toree.Main")
    command_args.append(toree_assembly)
    command_args.extend(toree_env_opts.split() if toree_env_opts else [])
    command_args.extend(toree_args)

    sys.stderr.write("\nSpark application log path: " + log_path + "\n\n")

    py4j = find_py4j(spark_home)
    if py4j:
        os.environ['PYTHON_PATH'] = "{spark_home}/python:{spark_home}/python/lib/{py4j}".format(spark_home=spark_home, py4j=py4j)
    os.execv(command, command_args)

if __name__ == '__main__':
    # remove the command name
    main(sys.argv[2:])
