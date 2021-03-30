"""
This CLI depends on having valid AWS credentials active, to query the account environment,
and on the environment variable CORE_STACK_NAME, which defaults to 'bclaw-core' if not set.
...or does it?  #todo
"""

import argparse
import logging
import yaml
import sys

from pkg.compiler import compile_template

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("infile", type=argparse.FileType("r"), nargs="?", default=sys.stdin)
    parser.add_argument("cfn_file", type=argparse.FileType("w"), nargs="?", default=sys.stdout)
    parser.add_argument("sfn_file", type=argparse.FileType("w"), nargs="?", default=sys.stderr)
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))

    wf_spec = yaml.safe_load(args.infile)
    wf_spec.pop("Transform", None)

    result = compile_template(wf_spec, state_machine_out=args.sfn_file)
    yaml.safe_dump(result, args.cfn_file)

    sys.exit(0)
