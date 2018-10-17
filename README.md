# VMDrill Storage Plugin

VMDrill is a storage plugin for [Apache
Drill](https://drill.apache.org/) , an open-source SQL query engine for
Big Data exploration.

This software module connects a Drill instance to an InfoVista&reg;
VistaMart&reg; server.

For the installation of Apache Drill, please refer to https://drill.apache.org/docs/install-drill/.

## Installation

The following jar files must be placed in the \<Apache-Drill-Installation-Directory\>/jars/3rdparty directory:
* **vm-drill-\<version\>.jar** available under https://github.com/infovista/VMDrill/releases
* **datamodel-ws-v8.jar** that can been found in the jar subdirectory of an
InfoVista&reg; VistaMart&reg; installation

If Apache Drill is already running, it must be restarted.

## Configuration

1. Open Drill web console and go to the storage tab: http://<IP address>:8047/storage

   Enter a name in the **New Storage
Plugin** section (this is the schema associated to the VistaMart&reg;
instance) and click \"Create\".

2. Fill in the Configuration:

   VMDrill configuration follows the following JSon syntax (remove characters 
\'\<\' and \'\>\'

   ```json
   {
   "type": "vm",
   "vistamartServer": "<VistaMart Server Host Name>",
   "vm_user": "<VistaMart operator user>",
   "vm_password": "<user password>",
   "enabled": true,
   "pageSize": 1000
   }
   ```

   - **pageSize** is the number of rows in the VistaMart&reg; response to
    each call. If more rows are needed, more requests are sent
    transparently to the VistaMart&reg; Server. Default is 1000.
   - **Type** is required and must be \"vm\"; this is how Drill matches
    the configuration to the BI Bridge Plugin module
   - **vistamartServer:** VistaMart&reg; server host name
   - **vm\_user:** VistaMart&reg; user name (see VistaMart&reg; guide)
   - **vm\_password:** the password of the operator
   - **enabled**: must be set to true, if not the plugin will not be
    visible from drill.
  
3. Click *Create*.

The new storage plugin should be visible and SQL queries can be launched
from Query tab.

For example, to get the list of tables in a schema named \'vm\' enter
the following request in the input field:

SELECT TABLE\_NAME from INFORMATION\_SCHEMA.\`TABLES\` where
TABLE\_SCHEMA = \'vm\'

## Schema Description

A configuration of the Storage Plugin is linked to one VistaMart&reg;.
Several configurations of the plugin can be created with different names
to access more than one VistaMart&reg;.

This name is in fact the schema name against which data can be requested.

A VMDrill schema contains one table for each Top Vista (VistaMart&reg;
meaning) for which at least one instance exists in the linked VistaMart&reg;
database. The table name is the Vista Name (e.g.: Router, 2G Cell)

Each table has the following structure:

- **timePeriod** (display Rate)
- **dateTime** (timestamp)
- **insId**
- **insTag**
- **insName**
- **proxyOf**
- **\<property name\>**  
**.** **.** **.**
- **\<property name\>**
- **\<indicator label\>**  
**.** **.** **.**
- \<**indicator label**\>

When requesting data from a table, the field timePeriod must be set in
the clause WHERE:

For instance, the following request is a valid one:

SELECT \* FROM Router WHERE timePeriod = \'H\' ;

Valid values for timePeriod field are **\'Y\', \'Q\', \'M\', \'W\',
\'D\', \'H\', \'30m\', \'15m\', \'10m\', \'5m\', \'1m\', \'15s\'**
