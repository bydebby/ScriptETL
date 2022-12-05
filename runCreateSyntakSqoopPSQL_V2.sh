. configControlTable.cfg
kinit -k -t apps.keytab apps/admin@BPOMDL.GO.ID

mysql -h${IP_DB} -u${USERCONTROL_DB} -p${PASSCONTROL_DB} -D${SCHEMACONTROL_DB} -P${PORTCONTROL_DB} -e "select CONCAT_WS(\"=\",config_name,value_config) from control_master_configs where int_proc=1" | grep -v CONCAT> initConfig.cfb

. initConfig.cfb

oozieId=$1
tableNameDelta=$2
dirConfig=$3
namefileETL=$4
tempSqoopFile=$5

#20181122|90|wf_M_TANGGAL|COLLECTION|M_TANGGAL|1|20|xxxxxxxxx|1

periode=`cat ${namefileETL} | awk -F"|" '{print $1}'`
id_trans=`cat ${namefileETL} | awk -F"|" '{print $2}'`
idDb=`cat ${namefileETL} | awk -F"|" '{print $6}'`
tname=`cat ${namefileETL} | awk -F"|" '{print $5}'`
mapper=`cat ${namefileETL} | awk -F"|" '{print $7}'`
dropTable=`cat ${namefileETL} | awk -F"|" '{print $9}'`
sequenceProc=`cat ${namefileETL} | awk -F"|" '{print $8}'`
flagPeriodic=`cat ${namefileETL} | awk -F"|" '{print $10}'`

killProses()
{
tableControl=$1
pesanError=$2
echo "$pesanError"
updateSqlFileErr=updateControlTable_error.sql
echo "update $tableControl set status = 3, notes = '${pesanError}' where oozie_id = '$oozieId' and periode = $periode and id_trans = $idtrans and status=2" > $updateSqlFileErr

mysql -h${IP_DB} -u${USERCONTROL_DB} -p${PASSCONTROL_DB} -D${SCHEMACONTROL_DB} -P${PORTCONTROL_DB} < $updateSqlFileErr
oozie job -oozie ${oozie_server} -kill $oozieId
exit 0
}

mysql -h${IP_DB} -u${USERCONTROL_DB} -p${PASSCONTROL_DB} -D${SCHEMACONTROL_DB} -P${PORTCONTROL_DB} -e "select concat_ws('|',app_name, ip_db,port_db,nama_db,schema_db,user_db,pass_db,source_schema) from control_master_databases where id_db=${idDb}" | grep -v concat > configDatabases.dat

#eregPangan|172.16.1.39\pkp|1433|ereg|dbo|sso|'it@bpom2018!@#'

initialDB=`cat configDatabases.dat | awk -F"|" '{print $1}'`
ipAddr=`cat configDatabases.dat | awk -F"|" '{print $2}'`
portNum=`cat configDatabases.dat | awk -F"|" '{print $3}'`
dataBaseName=`cat configDatabases.dat | awk -F"|" '{print $4}'`
schemaName=`cat configDatabases.dat | awk -F"|" '{print $5}'`
userName=`cat configDatabases.dat | awk -F"|" '{print $6}'`
passName=`cat configDatabases.dat | awk -F"|" '{print $7}'`
sourceSchema=`cat configDatabases.dat | awk -F"|" '{print $8}'`
#homeDir=`cat masterDatabases.dat | grep "homeDir" | awk -F"=" '{print $2}'`

updateSqlFile=updateControlTable.sql
#UPDATE STATUS CONTROL TABLE#
if [ \"${flagPeriodic}\" == \"1\" ]; then
   tblControl=control_process_daily_dsb
   echo "update $tblControl set status = 2, notes = 'RUNNING' where periode = $periode and id_trans = $id_trans and seq_process = '${sequenceProc}' and oozie_id = '$oozieId'" > $updateSqlFile
else
   tblControl=control_process_daily
   echo "update $tblControl set status = 2, notes = 'RUNNING',seq_process = '${sequenceProc}', oozie_id = '$oozieId' where periode = $periode and id_trans = $id_trans" > $updateSqlFile
fi

echo $tblControl

hadoop fs -put -f $updateSqlFile ${dirConfig}
mysql -h${IP_DB} -u${USERCONTROL_DB} -p${PASSCONTROL_DB} -D${SCHEMACONTROL_DB} -P${PORTCONTROL_DB} < $updateSqlFile
MYSQL_RETVAL=$?
if [ ${MYSQL_RETVAL} -ne 0 ]; then
   hadoop fs -put -f $updateSqlFile ${dirConfig}/${updateSqlFile}_error_${oozieId}
fi
#########################

tempSqoop=`basename ${tempSqoopFile}`

startDate=`date +%Y%m%d%H%M%S`

xtable=${dataBaseName}_${tname}
listColumn=listColumn_${xtable}.dat

if [ -s ${listColumn} ]; then
FIRSTCOLUMN=`cat ${listColumn} | awk -F"|" '{print "\x22"$1"\x22"}' | head -1`
LISTCOLUMNS=`cat ${listColumn} | awk -F"|" '{if ($2 == "json" || $2 == "jsonb" || $2 == "inet") print "cast(\x22"$1"\x22 as text) \x22"$1"\x22" ;else print "\x22"$1"\x22"}' | awk '!a[$0]++' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/,/g'`
LISTID=`cat ${listColumn} | awk -F"|" '{if ($4 == "PRIMARY KEY") print $1"_id"}' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/,/g'`
LISTADD=`cat ${listColumn} | awk -F"|" '{if ($4 == "PRIMARY KEY") print "\x22"$1"\x22 as \x22"$1"_id\x22"}' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/,/g'`

cat ${listColumn} | awk -F"|" '{if ($4 == "PRIMARY KEY") print $0}' > cekPrimary.dat

if [ -s cekPrimary.dat ]; then
cat ${tempSqoop} | sed "s/#DATABASENAME/${sourceSchema}/g" | sed "s/#SCHEMANAME/${schemaName}/g" | sed "s/#TNAME/${tname}/g" | sed "s/#XTABLE/${xtable}/g" | sed "s|#IP|${ipAddr}|g" | sed "s/#PORT/${portNum}/g" | sed "s/#USERNAME/${userName}/g" | sed "s/#PASSNAME/${passName}/g" | sed "s/#COLUMNNAME/${LISTCOLUMNS},${LISTADD}/g" | sed "s/#COLUMNKEY/${LISTID}/g" | sed "s/#MAPPER/${mapper}/g" | sed "s/#FIRSTCOLUMN/${FIRSTCOLUMN}/g" > syntak_${tname}.par
else
cat ${tempSqoop} | sed "s/#DATABASENAME/${sourceSchema}/g" | sed "s/#SCHEMANAME/${schemaName}/g" | sed "s/#TNAME/${tname}/g" | sed "s/#XTABLE/${xtable}/g" | sed "s|#IP|${ipAddr}|g" | sed "s/#PORT/${portNum}/g" | sed "s/#USERNAME/${userName}/g" | sed "s/#PASSNAME/${passName}/g" | sed "s/#COLUMNNAME/${LISTCOLUMNS}/g" | sed "s/#COLUMNKEY/${LISTID}/g" | sed "s/#MAPPER/${mapper}/g" | sed  "s/#FIRSTCOLUMN/${FIRSTCOLUMN}/g" > syntak_${tname}.par
fi

hadoop fs -put -f syntak_${tname}.par $dirConfig

if [ $dropTable -eq 1 ]; then
   echo "disable '${xtable}'" | hbase shell -n
   echo "drop '${xtable}'" | hbase shell -n
fi

echo "create '${xtable}','fam_${xtable}'" | hbase shell -n
else
  echo "file ${listColumn} tidak ada"
  killProses "file ${listColumn} tidak ada" $tblControl
fi

endDate=`date +%Y%m%d%H%M%S`

