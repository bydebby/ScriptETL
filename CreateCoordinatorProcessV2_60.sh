nameNode=hdfs://yavabpom
NmProcess=$1
listProcess=$2
addMinute=$3      #if 0 = 00,30
                  #if 1 = 01,31   
                  #dst

preqProcess=60
rootDir=${nameNode}/user/apps/bpom/BUNDLE/${NmProcess}_${preqProcess}
dirConfig=${rootDir}/config

dirTransform=${rootDir}/transform
dirLoading=${rootDir}/loading

jobProp=job_Dash_${NmProcess}_${preqProcess}.properties
bundleFileName=bundleConfiguration.xml
wfDelete=workflow_deletePath.xml

startDatex=`date +%Y"-"%m"-"%d`

countx=0
if [ -s coordinatorTemp.xml_temp ]; then
   rm coordinatorTemp.xml_temp
fi

if [ -s workflow_temp.xml ]; then
   rm workflow_temp.xml
fi

if [ -s ${jobProp} ]; then
   rm $jobProp
fi

if [ -s $bundleFileName ]; then
    rm $bundleFileName
fi

if [ -s $wfDelete ]; then
   rm $wfDelete
fi

echo "<bundle-app name='BundleApp_${NmProcess}' xmlns='uri:oozie:bundle:0.2'>
  <controls>
       <kick-off-time>\${startTime}</kick-off-time>
  </controls>" > $bundleFileName

echo "nameNode=hdfs://yavabpom
#jobTracker=yarnRM
jobTracker=masterbtkdi02.bpom.go.id:8032
oozie.use.system.libpath=true
queueName=default
user.name=apps

Schema_db=asrot
mainProcess=$NmProcess
rootDir=\${nameNode}/user/apps/bpom/BUNDLE/${NmProcess}_${preqProcess}
dirConfig=\${rootDir}/config

dirTransform=\${rootDir}/transform
dirLoading=\${rootDir}/loading
" > $jobProp

for xProcessList in `cat $listProcess` ; do
   echo $xProcessList
   countx=`expr $countx + 1` 
   xProcess=`echo $xProcessList | awk -F"|" '{print $1}'`
   xFlagPrevCheck=`echo $xProcessList | awk -F"|" '{print $2}'`
   xFlagDelete=`echo $xProcessList | awk -F"|" '{print $4}'`
   xFlagLoad=`echo $xProcessList | awk -F"|" '{print $6}'`
   if [ $xFlagLoad -eq 0 ]; then
      dirWorkflowProcessx=${dirTransform}/$xProcess/config
   else
      dirWorkflowProcessx=${dirLoading}/$xProcess/config
   fi
   hadoop fs -test -d $dirWorkflowProcessx
   if [ $? -ne 0 ]; then
     hadoop fs -mkdir -p $dirWorkflowProcessx
   fi
   
   if [ -s coordinator.xml_new ]; then
      rm coordinator.xml_new
   fi
   if [ $xFlagDelete -eq 0 ]; then
      if [ $xFlagLoad -eq 1 ]; then
         wfTemProcess=workflow_loadTemp.xml
      else
         wfTemProcess=workflow_transform.xml
      fi
      cat $wfTemProcess | sed "s|#table|processName${countx}|g" | sed "s|#rootDir|rootDir${countx}|g" | sed "s|#dirConfig|dirWorkflowProcess${countx}|g" | sed "s|#etlTransResult|etlTransResult${countx}|g" | sed "s|#locETLTransResult|locETLTransResult${countx}|g" | sed "s|#etlPrevTransResult|etlPrevTransResult${countx}|g" | sed "s|#hivexmlDir|hivexmlDir${countx}|g" | sed "s|#scriptHql|scriptHql${countx}|g" > workflow_temp.xml
      echo "hadoop fs -put -f workflow_temp.xml $dirWorkflowProcessx/workflow.xml"
      hadoop fs -put -f workflow_temp.xml $dirWorkflowProcessx/workflow.xml
      hadoop fs -put -f hive-site.xml $dirWorkflowProcessx
   fi
   if [ $xFlagDelete -eq 1 ]; then
echo "<workflow-app xmlns='uri:oozie:workflow:0.5' name='daily-DeleteLog_${xProcess}-wf'>
<global>
    <job-tracker>\${jobTracker}</job-tracker>
    <name-node>\${nameNode}</name-node>
    <configuration>
        <property>
            <name>mapred.job.queue.name</name>
            <value>\${queueName}</value>
        </property>
    </configuration>
</global>

 <start to=\"createCheckLog\"/>

 <action name=\"createCheckLog\">
        <shell xmlns=\"uri:oozie:shell-action:0.1\">
              <job-tracker>\${jobTracker}</job-tracker>
              <name-node>\${nameNode}</name-node>
                 <exec>\${checkLogDelete}</exec>
                 <argument>\${wf:id()}</argument>
                 <argument>${dirConfig}</argument>
                 <argument>\${mainProcess}</argument>
                 <env-var>HADOOP_USER_NAME=\${wf:user()}</env-var>
                 <file>\${initConfig}</file>
                 <file>\${checkLogDelete}</file>
                  <file>\${keytab_job}</file>
              <capture-output/>
        </shell>
        <ok to=\"pre-deleteFile-act\"/>
        <error to=\"fail\" />
  </action>

  <action name=\"pre-deleteFile-act\">
     <fs>
" > $wfDelete
      for xloopDelFile in `echo $xProcessList | awk -F"|" '{print $5}' | sed 's/,/ /g'` ; do
         fileNameDel=`cat $listProcess | awk -F"|" '{print $1}' | head -$xloopDelFile | tail -1` 
         xFlagLoady=`cat $listProcess | awk -F"|" '{print $6}' | head -$xloopDelFile | tail -1` 
         if [ $xFlagLoady -eq 1 ]; then
            dirTransFilePrev=/user/apps/bpom/BUNDLE/${NmProcess}_${preqProcess}/loading/${fileNameDel}/config/ETL_PREVLOADING_${fileNameDel}.SQL.RESULT
         else
            dirTransFilePrev=/user/apps/bpom/BUNDLE/${NmProcess}_${preqProcess}/transform/${fileNameDel}/config/ETL_PREVTRANSFORM_${fileNameDel}.SQL.RESULT
         fi
         hadoop fs -test -f $dirTransFilePrev
         if [ $? -eq 0 ]; then
            hadoop fs -rm -skipTrash $dirTransFilePrev
         fi
echo "        <delete path='$dirTransFilePrev'/> " >> $wfDelete
      done
echo "        <delete path='$dirConfig/lockDeleteLogBundle.lck'/> " >> $wfDelete
echo "     </fs>
        <ok to=\"end\"/>
        <error to=\"fail\" />
  </action>

  <kill name = \"fail\">
        <message>Job failed</message>
  </kill>

  <end name='end' />
</workflow-app>" >> $wfDelete

hadoop fs -put -f $wfDelete $dirTransform/$xProcess/config/workflow.xml
   fi

if [ $xFlagLoad -eq 0 ]; then
      echo "  <coordinator name='CoordApp-transform_${xProcess}' >
              <app-path>\${dirWorkflowProcess${countx}}</app-path>
             </coordinator>" >> $bundleFileName
fi
if [ $xFlagLoad -eq 1 ]; then
      echo "  <coordinator name='CoordApp-loading_${xProcess}' >
              <app-path>\${dirWorkflowProcess${countx}}</app-path>
              </coordinator>" >> $bundleFileName
fi

if [ $xFlagLoad -eq 1 ] ; then
echo "processName${countx}=$xProcess
dirWorkflowProcess${countx}=\${dirLoading}/\${processName${countx}}/config
rootDir${countx}=\${dirLoading}/\${processName${countx}}
etlTransResult${countx}=ETL_LOADING_\${processName${countx}}.SQL.RESULT
locETLTransResult${countx}=\${dirWorkflowProcess${countx}}/ETL_LOADING_\${processName${countx}}.SQL.RESULT
etlPrevTransResult${countx}=ETL_PREVLOADING_\${processName${countx}}.SQL.RESULT
locETLPrevTransResult${countx}=\${dirWorkflowProcess${countx}}/ETL_PREVLOADING_\${processName${countx}}.SQL.RESULT
hivexmlDir${countx}=\${dirWorkflowProcess${countx}}/hive-site.xml
scriptHql${countx}=hiveLoad_\${processName${countx}}.hql
scriptHqlDir${countx}=\${dirWorkflowProcess${countx}}/hiveLoad_\${processName${countx}}.hql
" >> $jobProp
else 
echo "processName${countx}=$xProcess
dirWorkflowProcess${countx}=\${dirTransform}/\${processName${countx}}/config
rootDir${countx}=\${dirTransform}/\${processName${countx}}
etlTransResult${countx}=ETL_TRANSFORM_\${processName${countx}}.SQL.RESULT
locETLTransResult${countx}=\${dirWorkflowProcess${countx}}/ETL_TRANSFORM_\${processName${countx}}.SQL.RESULT
etlPrevTransResult${countx}=ETL_PREVTRANSFORM_\${processName${countx}}.SQL.RESULT
locETLPrevTransResult${countx}=\${dirWorkflowProcess${countx}}/ETL_PREVTRANSFORM_\${processName${countx}}.SQL.RESULT
" >> $jobProp
fi

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

      echo "<coordinator-app name=\"CoordApp-$xProcess\"
            frequency=\"${menit} 05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21 * * *\"
                 start=\"\${startTime}\"
                 end=\"\${endTime}\"
                 timezone=\"\${timeZoneDef}\"
                 xmlns=\"uri:oozie:coordinator:0.2\">
       " > coordinator.xml_new
   if [ $xFlagPrevCheck -eq 1 ]; then
      echo "         <datasets>" >> coordinator.xml_new
      for xloop in `echo $xProcessList | awk -F"|" '{print $3}' | sed 's/,/ /g'` ; do
echo "                <dataset name=\"inputDS${xloop}\" frequency=\"\${coord:minutes(2)}\" initial-instance=\"\${startTime}\" timezone=\"\${timeZoneDef}\">
                        <uri-template>\${dirWorkflowProcess${xloop}}</uri-template>
                        <done-flag>\${etlPrevTransResult${xloop}}</done-flag>
                </dataset>" >> coordinator.xml_new
      done  
      echo "        </datasets>" >> coordinator.xml_new
      echo "        <input-events>" >> coordinator.xml_new
      for xloop2 in `echo $xProcessList | awk -F"|" '{print $3}' | sed 's/,/ /g'` ; do
          
echo "                <data-in name=\"CoordAppTrigDepInput${xloop2}\" dataset=\"inputDS${xloop2}\">
                     <start-instance>\${coord:current(-23)}</start-instance>
                     <end-instance>\${coord:current(0)}</end-instance>
                </data-in> " >> coordinator.xml_new       
      done
      echo "        </input-events>" >> coordinator.xml_new
   fi
echo "        <action>
                <workflow>
                        <app-path>$dirWorkflowProcessx</app-path>
                </workflow>
        </action>
</coordinator-app>" >> coordinator.xml_new


   echo "hadoop fs -put -f coordinator.xml_new $dirWorkflowProcessx/coordinator.xml"
   hadoop fs -put -f coordinator.xml_new $dirWorkflowProcessx/coordinator.xml
done

echo "</bundle-app>" >> $bundleFileName

hadoop fs -mkdir -p $dirConfig
hadoop fs -put -f $bundleFileName $dirConfig/bundleConfiguration.xml
hadoop fs -put -f hive-site.xml $dirConfig
echo "#oozie.service.WorkflowAppService.system.libpath=\${nameNode}\${rootDir}/config
oozie.bundle.application.path=\${dirConfig}/bundleConfiguration.xml

#oozie.wf.application.path=\${nameNode}\${rootDir}/config
#oozie.libpath=\${nameNode}/user/apps/Glib/lib/247

##Gen Script Daily
dirLibs=\${nameNode}/user/apps/Glib/lib
shLibs=\${dirLibs}/sh
jarLibs=\${dirLibs}/jar
keytab_job=\${shLibs}/apps.keytab
initConfig=\${shLibs}/configControlTable.cfg

#Transform
checkControlTable=\${shLibs}/checkControlTable_TRA.sh
settingHgridConf=\${shLibs}/CheckPrevAndCollection.sh
updateTblAftTrans=\${shLibs}/updateAfterTransform_TRA.sh
mainScriptLogHgrid=\${shLibs}/main_script_log_hgrid.sh
RunHgridScript=\${shLibs}/CreateRunSpark2.sh
checkLogDelete=\${shLibs}/CreateLockDelete.sh

#load
checkControlTableLoad=\${shLibs}/check_control_loading.sh
#createHqlSyntax=\${shLibs}/CreateLoadBPOM.sh
createHqlSyntax=\${shLibs}/CreateLoadBPOM_drop.sh
updateTblAftLoad=\${shLibs}/update_tbl_after_loadingB.sh
scriptLoadShell=\${shLibs}/Load2Hive.sh

#Action Name
sqoopName=sqoop-act
transformName=transform-act
optionFile=syntax.par
hiveName=loading-act

startTime=#REPLACEDATE#T#REPLACETIME#+0700
#startTime=${startDatex}T08:00+0700
endTime=2999-12-16T23:40+0700
timeZoneDef=Asia/Jakarta
jdbcURL=jdbc:hive2://masterbtkdi01.bpom.go.id:10000/bpom
jdbcPrincipal=hive/_HOST@BPOMDL.GO.ID
#jdbcURL=jdbc:hive2://192.168.4.122:10500/default
#jdbcPrincipal=hive/192.168.4.122@BPOMDL.GO.ID
" >> $jobProp

cp job_Dash_${NmProcess}_${preqProcess}.properties job_Dash_${NmProcess}_${preqProcess}.tmp
