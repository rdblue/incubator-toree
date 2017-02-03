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

def main(args):
    spark_args, toree_args = split_args(args)
    java_options, spark_args = get_value(spark_args, '--driver-java-options')
    driver_java_options = " ".join([ jarg for jarg in ["-noverify", java_options] if jarg ])

    work_path = os.getenv('CURRENT_JOB_WORKING_DIR')
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

    # add spark args
    command_args.extend(spark_env_opts.split() if spark_env_opts else [])
    command_args.extend(spark_args)
    command_args.append("--conf")
    command_args.append("spark.log.path=" + str(work_path) + "/spark.log")
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

    os.execv(command, command_args)

if __name__ == '__main__':
    # remove the command name
    main(sys.argv[2:])
