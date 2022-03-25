FOLDER=$(pwd)

docker run \
  -v $FOLDER:$FOLDER \
  openlattice/pymethodic:v1.3-rc1 \
  all \
  $FOLDER/resources/rawdata \
  $FOLDER/resources/preprocessed \
  $FOLDER/resources/subsetted \
  $FOLDER/resources/output \
  --precision 3600

docker run \
  -v $FOLDER:$FOLDER \
  --entrypoint python \
  openlattice/pymethodic:v1.3-rc1 \
  $FOLDER/check_output.py --directory $FOLDER





