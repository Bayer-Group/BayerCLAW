#! /bin/sh

set -eu

SCRIPT=$(readlink -f "$0")
HOME_DIR=$(dirname "$SCRIPT")

# should work for most modern linux distros
# see https://www.howtoforge.com/how_to_find_out_about_your_linux_distribution
#   for handling older distros
DISTRO=$(. /etc/os-release; echo $ID)

case $DISTRO in
  debian|ubuntu)
    SUBDIR="debian"
    ;;
  centos|amzn|rhel|fedora)
    SUBDIR="centos"
    ;;
#  alpine)
#    SUBDIR="alpine"
#    ;;
  *)
    echo "unknown linux distribution: $DISTRO"
    false
    ;;
esac

set -v

CMD="${HOME_DIR}/${SUBDIR}/bclaw_runner"
exec $CMD "$@"
