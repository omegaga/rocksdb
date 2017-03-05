HOST="maas.cmcl.cs.cmu.edu"
HOST2="node4.maas"
USERNAME="jianhong"
REPO="rocksdb/"
rsync -rav . ${USERNAME}@${HOST}:~/${REPO}
ssh -A -t ${USERNAME}@${HOST} "rsync -rav ~/${REPO} ${USERNAME}@${HOST2}:~/${REPO}"
