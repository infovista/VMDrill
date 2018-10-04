# VMDrill

VMDrill is a software module for [Apache
Drill](https://drill.apache.org/) , an open-source SQL query engine for Big Data exploration.

Requests are sent to a Drill instance using standard ANSI SQL language.

VMDrill is a software module for Drill that connects a Drill instance to
an InfoVista© VistaMart© database.

[TOC]

## Introduction

### Overview

This document describes the VMDrill module and its installation.

It does not cover Drill installation described in Drill
documentation at <https://drill.apache.org/docs/install-drill/>

.

## VMDrill Storage Plugin Installation & Configuration

### Storage Plugin Installation

The software module is a jar file (vm-drill-\<version\>.jar) that must
be placed in the \<Apache-Drill-Installation-Directory\>/jars/3rdparty,
**datamodel-ws-v8.jar** must be placed in the same directory.

VMDrill Storage Plugin Configuration
------------------------------------

### Configuration Description

Storage Plugins configuration for Drill must follow a specific Json
format.

VMDrill configuration follows the following syntax (remove characters '
'\< and '\>'

{

\"type\": \"vm\",

\"vistamartServer\": \"\<VistaMart Server Host Name\>\",

\"vm\_user\": \"\<VistaMart operator user\>\",

\"vm\_password\": \<user password\>\",

\"enabled\": true,

\"pageSize\": 1000

}

-   **pageSize** is the number of rows in the VistaMart© response to
    each call. If more rows are needed, more requests are sent
    transparently to the VistaMart© Server. Default is 1000.

-   **Type** is required and must be "vm"; this is how Drill matches the
    configuration to the BI Bridge Plugin module

-   **vistamartServer:** VistaMart© server host name

-   **vm\_user:** VistaMart© operator user name (see VistaMart© guide)

-   **vm\_password:** the password of the operator

-   **enabled**: must be set to true, if not the plugin will not be
    visible fro

### Make VMDrill Visible from Drill

Open Drill web console and go to the storage tab.

Note that some default storages are enabled by default, you can disable
them.

If no storage named vm is present, enter a name in the **New Storage Plugin** section (this is the schema
associated to the VistaMart© instance) and click "Create".

Fill in the Configuration (syntax is the one described above)

Click *Create*.

The new storage plugin should be visible and SQL queries can be launched
from Query tab.

For example, to get the list of tables in a schema named 'vm' enter the
following request in the input field:

SELECT TABLE\_NAME from INFORMATION\_SCHEMA.\`TABLES\` where
TABLE\_SCHEMA = \'vm\'

## VMDrill Schema Description

A configuration of the Storage Plugin is linked to one VistaMart©.
Several configurations of the plugin can be created with different names
to access more than one VistaMart©.

This name is in fact the schema name under with data can be requested.

A VMDrill schema contains two kinds of tables: topology tables and data
tables. A topology table is created for each Top Vista (VistaMart©
meaning) for which at least one instance exists in the linked VistaMart©
database. The topology table name is the Vista Name (e.g.: Router, 2G
Cell)

The second kind of table is created only if at least one indicator is
configured for one instance in the database. Its name is the Vista Name
followed by "\_data" (ex: Router\_data).



### Topology Tables

A topology table has the following structure:

-   **tag: **
-   **name **
-   **id**
-   **proxyOf**
- **\<proprety name\>**
    **.**
    **.**
    **.**
-   **\<proprety name\>**



### Data Tables

A data table has the following structure:

-   **timePeriod** (display Rate)

-   **name**

-   **id**

-   **tag**

-   **dateTime** (timestamp)

-   \<**indicator**\>
    **.**
    **.**
    **.**
-   \<**indicator**\>

When requesting data from a data table, the field timePeriod must be set
in the clause WHERE:

For instance, the following request is a valid one:

SELECT \* FROM Router\_data WHERE timePeriod = 'H' ;

Valid values for timePeriod field are **'Y', 'Q', 'M', 'W', 'D', 'H',
'30m', '15m', '10m', '5m', '1m', '15s'**


