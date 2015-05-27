## Dr Elephant

### Compiling & testing locally

* To be able to build & run the application, download and install [Play framework 2.2.2](http://downloads.typesafe.com/play/2.2.2/play-2.2.2.zip).
* The pre-installed play command on our boxes will not work as it is configured to look at LinkedIns repos
* If this is your first time working with Dr. Elephant, take the deployed Hadoop jars and put them in the /lib directory:
    scp eat1-magicgw01.grid.linkedin.com:/export/apps/hadoop/latest/hadoop-core-*.jar ./lib/.
  NOTE: When building dr-elephant, this jar file should be removed or renamed. Otherwise, the hadoop-2 executable built will not work.
* To compile dr-elephant, run the script ./compile.sh. A zip file is created in the 'dist' directory.
* You can run dr-elephant on your desktop and have it point to a cluster. Do the following:
  - cd dist;unzip *.zip; # and then cd to the dr-elephant release directory created.
  - Copy the cluster's configuration onto a local directory on your machine.
  - Copy the hadoop distribution onto a local directory (or, build hadoop locally).
  - For Hadoop-2, the following setup is needed:
    export HADOOP_HOME=/path/to/project/hadoop/hadoop-dist/target/hadoop-2.3.0_li-SNAPSHOT
    export HADOOP_CONF_DIR=/path/to/config/etc/hadoop
    export PATH=$HADOOP_HOME/bin:$PATH  # because dr-elephant uses 'hadoop classpath' to load the right classes
    vim $HADOOP_CONF_DIR/mapred-site.xml
        # Make sure to set the job history server co-ordinates if it is not already set
        # <property><name>mapreduce.jobhistory.webapp.address</name><value>eat1-hcl0764.grid:19888</value></property>
  - Set up and start mysql locally on your box, the default port is 3306. Create a database called 'drelephant', if it does not exist.
  - You can now start dr-elephant with the following command (assuming you are running mysql locally on port 3306):
    ./bin/dr-elephant -Dhttp.port=8089 -Ddb.default.url="jdbc:mysql://ssubrama-ld1.linkedin.biz:3306/drelephant?characterEncoding=UTF-8" -Devolutionplugin=enabled -DapplyEvolutions.default=true
  - Note that you can add other properties with the -D option, as -Dprop.name=value
  - You can debug dr-elephant using eclispse or idea by adding the argument -Djvm_args="-Xdebug -Xrunjdwp:transport=dt_socket,address=8876,server=y,suspend=y"
    This will start dr-elephant and wait until you attach eclipse/idea to port 8876. You can then set breakpoints and debug interactively.


### DB Schema evolutions

When the schema in the model package changes, play will need to be ran to automatically apply the evolution.

* There is a problem with Ebean where it does not support something like @Index to generate indices for columns of interest
* So what we did to work around this is to manually add indices into the sql script.
* To do this, we needed to prevent the automatically generated sql to overwrite our modified sql.
* The evolution sql file must be changed (by moving or removing the header "To stop Ebean DDL generation, remove this comment and start using Evolutions") to make sure it does not automatically generate new sql.
* To re-create the sql file from a new schema in code:
	* Backup the file at ./conf/evolutions/default/1.sql
	* Remove the file
	* Run play in debug mode and browse the page. This causes EBean to generate the new sql file, and automatically apply the evolution.
	* Copy over the indices from the old 1.sql file
	* Remove the header in the sql file so it does not get overwritten
	* Browse the page again to refresh the schema to add the indices.

### Deployment on the cluster

* SSH into the machine
* sudo as elephant
* go to /export/apps/elephant/
* unzip the file
* cd dr-elephant*/bin
* To start: ./start.sh
* To stop: ./stop.sh
* To deploy new version, be sure to kill the running process first

### Adding new heuristics

* Create a new heuristic and test it.
* Create a new view for the heuristic for example helpMapperSpill.scala.html
* Add the details of the heuristic in the HeuristicConf.xml file. The HeuristicConf.xml file requires the following details for each heuristic:
  * heuristicname: Name of the heuristic. (e.g. Mapper Spill)
  * classname: This should be the fully qualified name of the class (e.g. com.linkedin.drelephant.analysis.mapreduce.heuristics.MapperSpillHeuristic)
  * viewname: This should be the fully qualified name of the view. ( e.g. views.html.helpMapperSpill )
*Run Doctor Elephant, it should now include the new heuristics.
