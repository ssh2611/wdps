PWD=$(pwd)
PYENV_PATH="/var/scratch2/wdps1907/py_env"
export TMPDIR="/var/scratch2/wdps1907/tmp/"
CACHE_DIR="/var/scratch2/wdps1907/cache"

#rm -rf $PYENV_PATH
mkdir -p $PYENV_PATH
cd $PYENV_PATH
mkdir -p  cache
mkdir - p tmp
ls
pwd
python3 -m pip install --user virtualenv
python3 -m virtualenv wdps1907_env
source /var/scratch2/wdps1907/py_env/wdps1907_env/bin/activate
pip --version
pip install sklearn --no-cache-dir
pip install nltk
pip install bs4
pip install spacy
python3 -m spacy download en_core_web_md
pip install findspark
ipp install pyspark
pip install requests
pip install sparqlwrapper
virtualenv --relocatable wdpws1907_env
pwd
rm -rf *.zip
rm -rf WDPS1907_ENV
zip -rq wdps1907_env.zip wdps1907_env
mkdir WDPS1907_ENV
cp -r wdps1907_env WDPS1907_ENV/wdps1907_env
ln -s $PY_ENV_PATH/wdps1907_env.zip wdps1907_env.zip
ln -s $PY_ENV_PATH/wdps1907_env wdps1907_env
ln -s $PY_ENV_PATH/WDPS1907 WDPS1907_ENV#
