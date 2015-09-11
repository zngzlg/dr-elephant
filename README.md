
    Copyright 2015 LinkedIn Corp.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.


# Dr Elephant

### Compiling & testing locally

#### Play Setup - One time
* To be able to build & run the application, download and install [Play framework 2.2.2](http://downloads.typesafe.com/play/2.2.2/play-2.2.2.zip).
* The pre-installed play command on our boxes will not work as it is configured to look at LinkedIns repos.
* Add the Play installation directory to the system path.

#### Hadoop Setup - One time
* Setup hadoop locally. You can find instructions to setup a single node cluster [here](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html).
* Export variable HADOOP\_HOME if you haven't already.  
```
export HADOOP\_HOME=/path/to/hadoop/home
export HADOOP\_CONF\_DIR=$HADOOP_HOME/etc/hadoop
```

* Add hadoop to the system path because dr-elephant uses _'hadoop classpath'_ to load the right classes.  
```
export PATH=$HADOOP_HOME/bin:$PATH
```

#### Mysql Setup - One time
* Set up and start mysql locally on your box.
* Create a database called 'drelephant'. Use root with no password.  
```
mysql -u root -p
mysql> create database drelephant;
```

#### Dr. Elephant Setup
* Start Hadoop and run the history server.
* To compile dr-elephant, run the compile script. A zip file is created in the 'dist' directory.  
```
./compile.sh
```
* Unzip the zip file in dist and change to the dr-elephant release directory created. Henceforth we will refer this as DR_RELEASE.  
```
cd dist; unzip dr-elephant\*.zip; cd dr-elephant\*
```
* If you are running dr-elephant for the first time after creating the database, you need to enable evolutions. To do so append _-Devolutionplugin=enabled_ and _-DapplyEvolutions.default=true_ to jvm\_props in elephant.conf file.  
```
vim ./app-conf/elephant.conf
jvm\_props="... -Devolutionplugin=enabled -DapplyEvolutions.default=true"
```
* To start dr-elephant, run the start script specifying a path to the application's configuration files.  
```
$DR\_RELEASE/bin/start.sh -appconf $DR\_RELEASE/../../app-conf
```
* To stop dr-elephant run,  
```
$DR\_RELEASE/bin/stop.sh
```
* The dr-elephant logs are generated in the 'dist' directory besides the dr-elephant release.  
```
less $DR\_RELEASE/../logs/elephant/dr_elephant.log
```

### DB Schema evolutions

When the schema in the model package changes, run play to automatically apply the evolutions.

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

* SSH into the cluster machine.
* Switch user to _'elephant'_  
```
sudo -iu elephant
```
* Change directory to elephant.  
```
cd /export/apps/elephant/
```
* Unzip the dr-elephant release and change directory to it.
* To start dr-elephant run the start script. The start script takes an optional argument to the application's conf directory. By default it uses _'/export/apps/elephant/conf'_   
```
./bin/start.sh
```
* To stop dr-elephant run,  
```
./bin/stop.sh
```
* To deploy new version, be sure to kill the running process first

### Adding new heuristics

* Create a new heuristic and test it.
* Create a new view for the heuristic for example helpMapperSpill.scala.html
* Add the details of the heuristic in the HeuristicConf.xml file.
    * The HeuristicConf.xml file requires the following details for each heuristic:
        * **applicationtype**: The type of application analysed by the heuristic. e.g. mapreduce or spark
        * **heuristicname**: Name of the heuristic.
        * **classname**: Fully qualified name of the class.
        * **viewname**: Fully qualified name of the view.
        * **hadoopversions**: Versions of Hadoop with which the heuristic is compatible.
* A sample entry in HeuristicConf.xml would look like,  
```
<heuristic>
        <applicationtype>mapreduce</applicationtype>
        <heuristicname>Job Queue Limit</heuristicname>
        <classname>com.linkedin.drelephant.mapreduce.heuristics.JobQueueLimitHeuristic</classname>
        <viewname>views.html.helpJobQueueLimit</viewname>
        <hadoopversions>
            <version>1</version>
        </hadoopversions>
</heuristic>
```
* Run Doctor Elephant, it should now include the new heuristics.
