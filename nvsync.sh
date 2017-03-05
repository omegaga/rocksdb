HOST="nvm"
HOST2="pm-sdv3"
USERNAME="jhli"
REPO="rocksdb/"
rsync -rav . ${USERNAME}@${HOST}:~/${REPO}
ssh -A -t ${USERNAME}@${HOST} "rsync -rav ~/${REPO} ${USERNAME}@${HOST2}:~/${REPO}"
