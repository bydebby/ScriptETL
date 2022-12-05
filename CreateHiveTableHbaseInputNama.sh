#script ini hanya untuk tabel2 yg sudah pernah di getSpec
#jika belum bisa di buat sendiri listColumn_*.dat nya

id_schema=$1
tName=$2
hiveName=$3

schema=`cat ../script/masterDatabases.dat | awk -F"|" -v var1=$id_schema '{if ($1 == var1) print $5}'`
schemaHive=`cat ../script/masterDatabases.dat | awk -F"|" -v var1=$id_schema '{if ($1 == var1) print $2}'`
schemaHbase=`cat ../script/masterDatabases.dat | awk -F"|" -v var1=$id_schema '{if ($1 == var1) print $9}'`

specFile=../SPEC/mysql/${schemaHive}/${schema}/listColumn_${schema}_${tName}.dat #listColumn_asrot_tm_trader_unmatch.dat
JumRec=`wc -l $specFile | awk '{print $1}'`

#Setting Family Name
famHbaseName=fam_${schemaHbase}_${tName}
#famHbaseName=fam_master_perusahaan

hiveTname=${tName}
hbaseTname=${schemaHbase}_${tName}

#4|newAero|172.16.1.48|3306|ereg_dev|medan|medan1234|20
xCounter=1
if [ -s addColumnFile.txt ]; then
   rm addColumnFile.txt
fi
varHbaseColumn=":key"
allVarHbase=""

echo "CREATE EXTERNAL TABLE bpom.${hiveName} ( " > syntakCreate_${hiveTname}.hql

for xColumn in `cat ${specFile} | awk -F"|" '{print $1}'` ; do
 if [ $xCounter -eq 1 ]; then
    echo "IDX string," >> syntakCreate_${hiveTname}.hql
    newColumn=`echo "$xColumn string,"`
    allVarHbase="$varHbaseColumn,${famHbaseName}:${xColumn}"
 else
    if [ $xCounter -eq $JumRec ]; then
       newColumn=`echo "$xColumn string"` 
       allVarHbase="$allVarHbase,${famHbaseName}:${xColumn}"
    else 
       newColumn=`echo "$xColumn string,"`
       allVarHbase="$allVarHbase,${famHbaseName}:${xColumn}"
    fi
 fi
 
 echo $newColumn >> syntakCreate_${hiveTname}.hql
 xCounter=`expr $xCounter + 1` 
done

echo ")
ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping'='$allVarHbase',
  'serialization.format'='1')
TBLPROPERTIES (\"hbase.table.name\"=\"${hbaseTname}\");" >> syntakCreate_${hiveTname}.hql

hive -f syntakCreate_${hiveTname}.hql
echo "DONE, cek result tabel hive = $hiveName"
