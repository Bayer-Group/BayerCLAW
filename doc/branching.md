# Branching in workflow executions

BayerCLAW offers two special step types to enable workflow branching at runtime: simple chooser steps and parallel
chooser steps.

## Simple chooser steps

Simple chooser steps are intended to perform small branching operations, such as skipping over a single step.
A simple chooser a list of [conditions](#writing-conditions), each with a corresponding target step name, and
a set of input files. The chooser reads the input files and uses the data therein to evaluate the conditions. 
The conditions are evaluated in the order that they appear in the list, and execution continues to the target of
the first condition that evaluates to True. If none of the conditions evaluates as True, execution continues with
the next step in the block.

```yaml
SimpleChooserStep:
  inputs:
    input1: file1.json
    input2: file2.json
  choices:
    # workflow will go to the `next` step of the first condition that evaluates to True

    # `input1.value1` and `input2.value2` refer to fields in the two input JSON files
    -
      if: input1.value1 > 0.5
      next: Step3
    -
      if: input2.value2 < 0.9
      next: Step4
    -
      # you can branch on values from the job data file too
      if: job.check_me == 1
      next: Step5
  # if none of the conditions evaluates to True, fall through to Step2

Step2:
  image: step2_image
  # etc.
  next: Step5

Step3:
  image: step3_image
  # etc.
  end: true

Step4:
  image: step4_image
  # etc.
  # no `next` or `end`...proceed to Step5

Step5:
  image: step5_image
  # etc.
```

Note how the `next` and `end` fields of the target steps are used to achieve different execution paths in this example:

`input1.value1 > 0.5`: Step3 -> stop execution

`input1.value1 <= 0.5` and `input2.value2 < 0.9`: 
Step4 -> Step5 -> ...

`input1.value1 <= 0.5`, `input2.value2 >= 0.9`, `job.check_me == 1`:
Step5 -> ...

`input1.value1 <= 0.5`, `input2.value2 >= 0.9`, `job.check_me != 1`:
Step2 -> Step5 -> ...

Note that a simple chooser step cannot directly stop the execution of a workflow. If you need to stop the execution
based on some condition, branch to a native Succeed or Fail step. See [the native steps documentation](./language.md#native-step-functions-steps)
for more information.

Your workflow must have at least one step after a simple chooser in order to accommodate the default fallthrough
behavior. Again, if you want to stop execution in this case, use a native Succeed or Fail step. 

If you have a simple chooser in the steps block of a scatter/gather or parallel step, it can only branch to
other steps inside of the same block. Similarly, a simple chooser cannot branch to a step that is nested
inside of a scatter/gather or parallel step.

## Parallel chooser steps

Parallel chooser steps are meant for situations where the workflow has to run (or not run) large blocks of steps
in response to conditions. A parallel chooser step takes a set of inputs and a list of branches, similar to a
[parallel native step](language.md#parallel-steps). Each branch has an associated condition which is evaluated
using the data in the input files. Unlike a simple chooser step, though, a parallel chooser will execute every
branch for which the associated condition evaluates to True.

```yaml
ParallelChooserStep:
  inputs:
    input1: file1.json
    input2: file2.json
  branches:
    # EVERY branch whose condition evaluates to True will be executed

    # Again, `input1.value1` and `input2.value2` are fields in the input files
    -
      if: input1.value1 > 0.5
      steps:
        -
          do_this:
            image: this_image
            # etc
          do_that:
            image: that_image
            # etc
    -
      if: input2.value2 < 0.9
      steps:
        -
          do_the_other:
            image: the_other_image
            # etc
    -
      # can check values in the job data file here too
      if: job.check_me == 1
      steps:
        -
          do_whatever:
            image: whatever_image
            # etc
    -
      # no `if` in this branch...always execute it
      steps:
        -
          always_do_this:
            image: always_do_this_image
            # etc
```

Execution of a branch can be stopped early by using a Succeed native step or by executing a step with an
`end` field. These will stop the branch, but the workflow will continue running. A Fail native step within a
branch, however, will terminate the entire execution with a FAILED status.
 

## Writing conditions

Conditions are written as Python expressions that evaluate to boolean values.

The input files for evaluating the conditions must be in JSON format. Values in the files can be accessed using
a [JMESPath-like](https://jmespath.org/tutorial.html) paths.

As an example, suppose we had the following input files:

_file1.json_: 
```json
{
  "a": 1,
  "b": {
    "d": -5,
    "e": 0.06
  },
  "c": true
}
```

_file2.json_:
```json
{
  "x": 10,
  "y": [2, 3, 4],
  "z": "sasquatch"
}
```

And suppose the `inputs` spec for our chooser (of either flavor) is:
```yaml
inputs:
    input1: file1.json
    input2: file2.json
``` 

Then all of the following conditions are valid, and would evaluate to True:
```pythonstub
# numeric expression
input1.a == 1

# boolean value
input1.c

# string expression
'q' in input2.z

# nested list lookup
input2.y[1] == 3

# nested object lookup
input1.b.d < 0

# dict key exists
'e' in input1.b

# list element exists
4 in input2.y

# compare values from different files
input1.a < input2.x

# math
input1.a + input2.x == 11

# chained comparison
7 < input2.x < 15

# logical expression
input1.a == 1 and input2.x == 10 

# builtin function call
abs(input1.b.d) > 3

# math module call
math.isclose(input1.b.e, 0.06)

# regular expression
re.match(r'sasq.*', input2.z) is not None 
```

If your chooser takes only one input file, you can leave the prefix (namespace) off of the path. So, for
instance, if the aforementioned _input1.json_ was the only input file, you could write a condition like `b.d == -5`

To access values from the job data file, use the prefix `job`. If your chooser works only with values from the
job data file, you can omit the `inputs` block from the chooser spec. You will still need to use the `job` prefix
in the conditions, though.


## Optional inputs

If workflow steps are skipped, the files they would have produced won't exist. Normally, this would be a problem
since BayerCLAW crashes executions when it can't find all of the necessary inputs for a step. To tell BayerCLAW
to ignore missing files, you can designate them as optional using a question mark:

```yaml
Chooser:
  choices:
    if: job.skip_step1
    next: Step2
  # ...else fall through

Step1:
  image: prefrobnicator
  commands:
    - prefrobnicate > ${output1}
  outputs:
    output1: file2.txt

Step2:
  image: frobnicator
  inputs:
    required1: file1.txt
    optional1?: file2.txt
    #        ^---this question mark makes it optional
  commands:
    - if [ ! -e ${optional1} ]; then touch ${optional1}; fi
    - frobnicate ${optional1} ${required1} > ${output1}
  outputs:
    output1: frobnicated.txt
```

NB: do not include the question mark in the commands.

If an optional file cannot be found in S3, no corresponding file -- not even an empty file -- will be placed in
the batch job's working directory. The code in the `commands` block needs to account for this.

Optional inputs are only available in batch steps.



# todo
how do filename globs work with optional inputs?
will input1.x work with single input?
check that no file gets placed in working dir