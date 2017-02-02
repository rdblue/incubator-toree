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

def main(args):
    spark_args, toree_args = split_args(args)

    spark_home = os.getenv('SPARK_HOME')
    spark_env_opts = os.getenv('SPARK_OPTS')
    toree_assembly = os.getenv('TOREE_ASSEMBLY')
    toree_env_opts = os.getenv('TOREE_OPTS')

    if not spark_home:
        raise StandardError("SPARK_HOME is not set")

    if not toree_assembly:
        raise StandardError("TOREE_ASSEMBLY is not set")

    command = "{spark}/bin/dsespark-submit.py".format(spark=spark_home)

    # dsespark-submit will remove the first arg
    command_args = ['scala-kernel']

    # add spark args
    command_args.extend(spark_env_opts.split() if spark_env_opts else [])
    command_args.extend(spark_args)

    # add toree args
    command_args.append("--class")
    command_args.append("org.apache.toree.Main")
    command_args.append(toree_assembly)
    command_args.extend(toree_env_opts.split() if toree_env_opts else [])

    os.execv(command, command_args)

if __name__ == '__main__':
    # remove the command name
    main(sys.argv[2:])
