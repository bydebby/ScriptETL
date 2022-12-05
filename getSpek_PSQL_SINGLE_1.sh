idDb=$1
xtable=$2

scriptDir=/home/apps/MIGRATION/COLLECTION/script
initialDB=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $2}'`
ipAddr=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $3}'`
portNum=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $4}'`
dataBaseName=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $5}'`
schemaName=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $6}'`
userName=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $7}'`
passName=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $8}'`
mapper=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $9}'`
homeDir=`cat ${scriptDir}/masterDatabases.dat | grep "homeDir" | awk -F"=" '{print $2}'`
hdfsDir=`cat ${scriptDir}/masterDatabases.dat | grep "hdfsDir" | awk -F"=" '{print $2}'`
databaseAsli=`cat ${scriptDir}/masterDatabases.dat | awk -F"|" -v varDb=$idDb '{if ($1 == varDb) print $10}'`

spekDir=${homeDir}/SPEC/PSQL_SERVER/${initialDB}/${dataBaseName}/${schemaName}

temSpecSqoop=${homeDir}/temp_par/PSQL_SERVER/syntak_getSpecTable
temPRIMARYSqoop=${homeDir}/temp_par/PSQL_SERVER/syntak_getPrimaryTable
temListTableSqoop=${homeDir}/temp_par/PSQL_SERVER/syntak_getAllTable.par

if [ ! -d ${spekDir} ]; then
   mkdir -p ${spekDir}
fi

createSqoop()
{
cat ${temSpecSqoop}.par | sed "s/#DATABASENAME/${databaseAsli}/g" | sed "s/#SCHEMANAME/${schemaName}/g" | sed "s/#TNAME/${xtable}/g" | sed "s|#IP|${ipAddr}|g" | sed "s/#PORT/${portNum}/g" | sed "s/#USERNAME/${userName}/g" | sed "s/#PASSNAME/${passName}/g" | sed "s|#HDFSDIR|${hdfsDir}|g" | sed "s|#INITIALDB|${initialDB}|g" > ${spekDir}/syntak_Spec_${xtable}.par
cat ${temPRIMARYSqoop}.par | sed "s/#DATABASENAME/${databaseAsli}/g" | sed "s/#SCHEMANAME/${schemaName}/g" | sed "s/#TNAME/${xtable}/g" | sed "s|#IP|${ipAddr}|g" | sed "s/#PORT/${portNum}/g" | sed "s/#USERNAME/${userName}/g" | sed "s/#PASSNAME/${passName}/g" | sed "s|#HDFSDIR|${hdfsDir}|g" | sed "s|#INITIALDB|${initialDB}|g" > ${spekDir}/syntak_Prim_${xtable}.par
}

cat ${temListTableSqoop} | sed "s/#DATABASENAME/${databaseAsli}/g" | sed "s/#SCHEMANAME/${schemaName}/g" | sed "s|#IP|${ipAddr}|g" | sed "s/#PORT/${portNum}/g" | sed "s/#USERNAME/${userName}/g" | sed "s/#PASSNAME/${passName}/g" | sed "s|#HDFSDIR|${hdfsDir}|g" | sed "s|#INITIALDB|${initialDB}|g" > ${spekDir}/syntak_ListTab_${dataBaseName}_${schemaName}.par
export HADOOP_CLASSPATH="${homeDir}/lib/postgresql-9.0-801.jdbc4.jar"
sqoop import --options-file ${spekDir}/syntak_ListTab_${dataBaseName}_${schemaName}.par
OutdirListTable=${hdfsDir}/LIST/${databaseAsli}/${schemaName}

if [ -s ${spekDir}/list_Table_${dataBaseName}_${schemaName}.dat ]; then
   rm ${spekDir}/list_Table_${dataBaseName}_${schemaName}.dat
fi
hadoop fs -getmerge ${OutdirListTable} ${spekDir}/list_Table_${dataBaseName}_${schemaName}.dat

later()
{
#for xlistTab in `cat ${spekDir}/list_Table_${dataBaseName}_${schemaName}.dat` ; do
  #xtable=$xlistTab
  createSqoop
  sqoop import --options-file ${spekDir}/syntak_Spec_${xtable}.par

  outdirHDFSSPEC=${hdfsDir}/SPEC/${initialDB}/${databaseAsli}/${schemaName}/${xtable}

  if [ -s ${spekDir}/listColumn_${dataBaseName}_${xtable}.dat ]; then
     rm ${spekDir}/listColumn_${dataBaseName}_${xtable}.dat
  fi
  hadoop fs -getmerge ${outdirHDFSSPEC} ${spekDir}/listColumn_${dataBaseName}_${xtable}.dat
  jumRow=`cat ${spekDir}/listColumn_${dataBaseName}_${xtable}.dat | awk -F"|" '{if ($4 != "") print $0}' | wc -l | awk '{print $1}'`
  if [ $jumRow -eq 0 ]; then
     echo "${dataBaseName}|${schemaName}|${xtable}" >> listNullPrimary_PSQlServer.dat 
  fi
#done
}

later

