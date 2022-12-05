appName=$1
schemaName=$2
listFile=$3
freq=$4
dateStart=$5
timeStart=$6
daily=$7
addMinute=$8

DirBundle=/user/apps/bpom/BUNDLE/collection/${schemaName}_${daily}

createDir()
{
hadoop fs -test -d $DirBundle/config
if [ $? -ne 0 ]; then
   hadoop fs -mkdir -p $DirBundle/config
fi

hadoop fs -put -f hbase-site.xml $DirBundle/config

for xDirTableConfx in `cat $listFile` ; do
    xDirTableConf=`echo $xDirTableConfx | awk -F"|" '{print $1}'` 
    hadoop fs -test -d $DirBundle/$xDirTableConf/config
    if [ $? -ne 0 ]; then
       hadoop fs -mkdir -p $DirBundle/$xDirTableConf/config 
    fi
    cp /home/apps/MIGRATION/COLLECTION/SPEC/PSQL_SERVER/$appName/$schemaName/public/listColumn_${schemaName}_${xDirTableConf}.dat .
    hadoop fs -put -f listColumn_${schemaName}_${xDirTableConf}.dat $DirBundle/$xDirTableConf/config
    rm listColumn_${schemaName}_${xDirTableConf}.dat
    hadoop fs -put -f hbase-site.xml $DirBundle/$xDirTableConf/config
done
}

createBundle()
{
bundlename=bundleConfiguration_DashColl_${schemaName}.xml

echo "<bundle-app name='BundleCollectionDashBoard_${schemaName}_${daily}' xmlns='uri:oozie:bundle:0.2'>
  <controls>
       <kick-off-time>\${startTime}</kick-off-time>
  </controls>" > hasil/$bundlename

xcountBund=1
for xTablex in `cat $listFile` ; do
    xTable=`echo $xTablex | awk -F"|" '{print $1}'`
    echo "  <coordinator name='CoordApp-${schemaName}.${xTable}' >
       <app-path>\${dirWorkflowProcess${xcountBund}}</app-path>
   </coordinator>" >> hasil/$bundlename
   xcountBund=`expr $xcountBund + 1` 
done
echo "</bundle-app>" >> hasil/$bundlename
hadoop fs -put -f hasil/$bundlename $DirBundle/config/bundleConfiguration.xml
}


createJobProp()
{
jobpropertiesName=job_BundleDashColl${schemaName}_${daily}.properties
echo "nameNode=hdfs://yavabpom
#jobTracker=yarnRM
jobTracker=masterbtkdi02.bpom.go.id:8032
oozie.use.system.libpath=true
queueName=default
user.name=apps

Schema_db=$schemaName
rootDir=/user/apps/bpom/BUNDLE/collection/\${Schema_db}_${daily}

jarFileLib=mysql-connector-java.jar
dirConfig=\${rootDir}/config
#oozie.libpath=\${nameNode}/user/apps/Glib/lib/247
oozie.bundle.application.path=\${dirConfig}/bundleConfiguration.xml 
"> $jobpropertiesName

xcountJob=1
for xTablecolx in `cat $listFile` ; do
     xTablecol=`echo $xTablecolx | awk -F"|" '{print $1}'`
     flagPrim=`echo $xTablecolx | awk -F"|" '{print $2}'`
     if [ $flagPrim -eq 1 ]; then
        sqoopTempFile=syntak_temp_psql1.par
     else
        sqoopTempFile=syntak_temp_psql_noKey2.par
     fi
     echo "table${xcountJob}=$xTablecol
rootDir${xcountJob}=\${rootDir}/\${table${xcountJob}}
dirWorkflowProcess${xcountJob}=\${rootDir${xcountJob}}/config
syntaxSpoolTemplate${xcountJob}=\${shLibs}/$sqoopTempFile
columnPar${xcountJob}=\${dirWorkflowProcess${xcountJob}}/listColumn_\${Schema_db}_\${table${xcountJob}}.dat
etlCollResult${xcountJob}=ETL_COLLECTION_\${table${xcountJob}}.SQL.RESULT
locETLCollResult${xcountJob}=\${dirWorkflowProcess${xcountJob}}/ETL_COLLECTION_\${table${xcountJob}}.SQL.RESULT
optionFile${xcountJob}=syntak_\${table${xcountJob}}.par
hbasexmldir${xcountJob}=\${dirWorkflowProcess${xcountJob}}/hbase-site.xml
" >> $jobpropertiesName
     xcountJob=`expr $xcountJob + 1` 
done
echo "##Gen Script Daily
dirLibs=\${nameNode}/user/apps/Glib/lib
shLibs=\${dirLibs}/sh
jarLibs=\${dirLibs}/jar
keytab_job=\${shLibs}/apps.keytab

initConfig=\${shLibs}/configControlTable.cfg
preSqoopSh=\${shLibs}/initiate_preproses.sh

##Gen Script Daily
checkControlTable=\${shLibs}/check_control_collection.sh
runSqoop=\${shLibs}/runCreateSyntakSqoopPSQL.sh
updateTblAftColl=\${shLibs}/update_tbl_after_collect.sh

hbasexml=hbase-site.xml
#Action Name
sqoopName=sqoop-act
hiveName=loading-hive
#HADOOP_CLASSPATH=mysql-connector-java-5.1.38-bin.jar
driverFile=\${dirLibs}/247/postgresql-9.0-801.jdbc4.jar
fileMetric=\${dirLibs}/247/metrics-core-2.2.0.jar
startTime=${dateStart}T${timeStart}+0700
endTime=2999-01-01T21:00+0700
timeZoneDef=Asia/Jakarta

zookeeper_quorum=masterbtkdi01.bpom.go.id,masterbtkdi02.bpom.go.id,yava00.bpom.go.id
hbase_master_principal=hbase-bpom_datalake@BPOMDL.GO.ID
hbase_region_server_principal=hbase/_HOST@BPOMDL.GO.ID
" >> $jobpropertiesName
}

if [ ! -d hasil ]; then
   mkdir hasil
fi

createDir
createJobProp
createBundle

xnumAll=""
for xNum in 00; do
   xnumNew=`expr $xNum + $addMinute`
   jumRec=`echo $xnumNew | awk '{print length($0)}'`
   if [ $jumRec -eq 1 ]; then
      xnumNew=0$xnumNew
   fi
   xnumAll=$xnumAll" "$xnumNew
done
menit=`echo $xnumAll | sed 's/ /,/g'`

county=1
rootDir=/user/apps/bpom/BUNDLE/collection/${schemaName}_${daily}
for xTablecoordx in `cat $listFile` ; do
   xTablecoord=`echo $xTablecoordx | awk -F"|" '{print $1}'` 
   if [ $freq -eq 1 ]; then
   cat coordinatorTemp.xml | sed "s|#ProcessName|${schemaName}_${xTablecoord}|g" | sed "s|#dirWorkflowProcess|$rootDir/$xTablecoord/config|g" > hasil/coordinator_${schemaName}_${xTablecoord}.xml
   else
   cat coordinatorTempX.xml | sed "s|#ProcessName|${schemaName}_${xTablecoord}|g" | sed "s|#dirWorkflowProcess|$rootDir/$xTablecoord/config|g" | sed "s|#MENIT|${menit}|g" > hasil/coordinator_${schemaName}_${xTablecoord}.xml
   fi
   cat tempworkflow.xml | sed "s|#SCHEMANAME|$schemaName|g" | sed "s|#TABLENAME|$xTablecoord|g" | sed "s|#table|table${county}|g" | sed "s|#rootDir|rootDir${county}|g" | sed "s|#dirConfig|dirWorkflowProcess${county}|g" | sed "s|#etlCollResult|etlCollResult${county}|g" | sed "s|#locETLCollResult|locETLCollResult${county}|g" | sed "s|#syntaxSpoolTemplate|syntaxSpoolTemplate${county}|g" | sed "s|#columnPar|columnPar${county}|g" | sed "s|#optionFile|optionFile${county}|g" | sed "s|#hbasexmldir|hbasexmldir${county}|g" > hasil/workflow_${schemaName}_${xTablecoord}.xml
   county=`expr ${county} + 1`
   hadoop fs -put -f hasil/coordinator_${schemaName}_${xTablecoord}.xml $DirBundle/$xTablecoord/config/coordinator.xml
   hadoop fs -put -f hasil/workflow_${schemaName}_${xTablecoord}.xml $DirBundle/$xTablecoord/config/workflow.xml
done
