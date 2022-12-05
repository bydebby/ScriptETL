idDb=$1
list=$2

for xx in `cat $list | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $2}'` ; do
   bash getSpek_PSQL_SINGLE_1.sh $idDb $xx
   echo "bash getSpek_PSQL_SINGLE_1.sh $idDb $xx"
done

