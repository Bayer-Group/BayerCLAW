# Advanced BayerCLAW options

## The `shell` option

BayerCLAW 1.1.3+ provides the ability to choose which Unix shell to run Batch job commands
under. You can specify the shell to use globally, using the setting in the `Options` block
or for individual steps in the `compute` block. The choices for the `shell` setting are
`sh`, `bash`, and `sh-pipefail`:

| Choice      | Shell | Shell options  | Default? |
|-------------|-------|----------------|----------|
| sh          | sh    | -veu           | yes      |
| bash        | bash  | -veuo pipefail | no       |
| sh-pipefail | sh    | -veuo pipefail | no       |

Bourne shell (`sh`) is for all intents and purposes supported by all Unix implementations,
so  it is the default. The `bash` choice is provided mostly for backward compatibility
but is still supported by most popular Linuxen (notably, Alpine Linux).

The shell options are based on the so-called [Bash Strict Mode](http://redsymbol.net/articles/unofficial-bash-strict-mode/)
as an aid to debugging. Note that the `pipefail` option is not included in the Bourne shell
specification (as of June 2022) so it is not included in the default shell options. Nevertheless,
some `sh` implementations (notably, again, Alpine Linux) do provide a `pipefail` option, 
hence the `sh-pipefail` choice. To check whether `pipefail` is implemented in your favorite
`sh`, use the command `sh -c "set -o"` and look for a `pipefail` entry in the resulting list.

Note that the `-v` shell option is used to echo each command before execution. Some users
may prefer the similar `-x` option. The difference is that `-x` prints commands after
variable substitution has happened, which can cause privileged information (passwords,
etc.) to be exposed in the logs. With `-v`, commands are printed before variable substitution,
and thus is the safer choice.
