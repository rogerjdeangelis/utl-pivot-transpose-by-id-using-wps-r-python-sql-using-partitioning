%let pgm=utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning;

Pivot transpose by id in wps r python sql using partitions;

  SOLUTIONS

     1 wps proc transpose
     2 wps sql partitions
     3 wps r sql partitions
     4 wps r base no sql
     5 wps python sql
       Slight problem with python, python converts sqllite NULLS to None.
       R converts sqllite to NULLs to <NA> and WPS convers <NA> to WPS
       missings "".

Macro sqlPartitions on end and in

github
https://tinyurl.com/5xayn38v
https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning

stackoverflow
https://tinyurl.com/2srr6fdf
https://stackoverflow.com/questions/77317939/how-to-get-rid-of-duplicate-rows-preserving-information-into-another-column-in-r

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/
options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
 informat id $1. color $5.;
 input id color;
cards4;
a red
a blue
b green
c blue
c green
d red
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                                                                                                                        */
/*       INPUT                  PROCESS                       OUTPUT                                                      */
/*                                                                                                                        */
/*  Obs    ID    COLOR                                   ID    COLOR1    COLOR2                                           */
/*                              pivot by id var color                                                                     */
/*   1     a     red                                     a     red       blue                                             */
/*   2     a     blue                                                                                                     */
/*   3     b     green                                   b     green                                                      */
/*   4     c     blue                                    c     blue      green                                            */
/*   5     c     green                                                                                                    */
/*   6     d     red                                     d     red                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                              _
/ | __      ___ __  ___   _ __  _ __ ___   ___ | |_ _ __ __ _ _ __  ___ _ __   ___  ___  ___
| | \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __|| __| `__/ _` | `_ \/ __| `_ \ / _ \/ __|/ _ \
| |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__ | |_| | | (_| | | | \__ \ |_) | (_) \__ \  __/
|_|   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| \__|_|  \__,_|_| |_|___/ .__/ \___/|___/\___|
             |_|         |_|                                           |_|
*/

%let _max=2;

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc transpose data=sd1.have out=sd1.want(drop=_name_) prefix=color;
  by id;
  var color;
run;quit;
proc print data=sd1.want;
run;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* You need to know the max number of colors in any id.                                                                   */
/* The max is 2 because no id has more that two colors.                                                                   */
/*                                                                                                                        */
/* You do not need to know the max for proc transpose.                                                                    */
/*                                                                                                                        */
/* %let _max = 2;                                                                                                         */
/*                                                                                                                        */
/* Obs    ID    COLOR1    COLOR2                                                                                          */
/*                                                                                                                        */
/*  1     a     red       blue                                                                                            */
/*  2     b     green                                                                                                     */
/*  3     c     blue      green                                                                                           */
/*  4     d     red                                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                   _                    _   _ _   _
|___ \  __      ___ __  ___   ___  __ _| |  _ __   __ _ _ __| |_(_) |_(_) ___  _ __  ___
  __) | \ \ /\ / / `_ \/ __| / __|/ _` | | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \/ __|
 / __/   \ V  V /| |_) \__ \ \__ \ (_| | | | |_) | (_| | |  | |_| | |_| | (_) | | | \__ \
|_____|   \_/\_/ | .__/|___/ |___/\__, |_| | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|___/
                 |_|                 |_|   |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%let _max=2;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
options sasautos=('c:/otowps' sasautos);
proc sql;
  create
       table sd1.want as
  select
      id
     ,max(case when partition=1     then color else '' end) as color1
     ,max(case when partition=&_max then color else '' end) as color&_max
  from
      %sqlpartition(sd1.have,by=id)
  group
      by id
;quit;
proc print data=sd1.want;
run;quit;
");

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    ID    color1    color2                                                                                          */
/*                                                                                                                        */
/*  1     a     red       blue                                                                                            */
/*  2     b     green                                                                                                     */
/*  3     c     blue      green                                                                                           */
/*  4     d     red                                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                         _                    _   _ _   _
|___ /  __      ___ __  ___   _ __   ___  __ _| |  _ __   __ _ _ __| |_(_) |_(_) ___  _ __  ___
  |_ \  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \/ __|
 ___) |  \ V  V /| |_) \__ \ | |    \__ \ (_| | | | |_) | (_| | |  | |_| | |_| | (_) | | | \__ \
|____/    \_/\_/ | .__/|___/ |_|    |___/\__, |_| | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|___/
                 |_|                        |_|   |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%let _max=2;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
   select
      id
     ,max(case when partition=1     then color else NULL end) as color1
     ,max(case when partition=&_max then color else NULL end) as color&_max
   from
      (select id, color, row_number() OVER (PARTITION BY id) as partition from have )
   group
     by id
');
want;
endsubmit;
import data=sd1.want r=want;
proc print data=sd1.want width=min;
run;quit;
");

/*  _                                 _                                             _
| || |   __      ___ __  ___   _ __  | |__   __ _ ___  ___   _ __   ___   ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` / __|/ _ \ | `_ \ / _ \ / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |    | |_) | (_| \__ \  __/ | | | | (_) |\__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ |_|    |_.__/ \__,_|___/\___| |_| |_|\___/ |___/\__, |_|
                  |_|                                                            |_|

*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%let _max=2;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(dplyr);
library(tidyr);
want<-have %>%
  mutate(n = row_number(),
         .by = ID) %>%
  pivot_wider(
    names_from = n,
    names_prefix = 'COLOR_',
    values_from = COLOR
  );
want;
endsubmit;
import data=sd1.want r=want;
proc print data=sd1.want width=min;
run;quit;
");

proc print data=sd1.want width=min;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS R                                                                                                             */
/*                                                                                                                        */
/*  # A tibble: 4 x 3                                                                                                     */
/*    ID    COLOR_1 COLOR_2                                                                                               */
/*    <chr> <chr>   <chr>                                                                                                 */
/*  1 a     red     blue                                                                                                  */
/*  2 b     green   <NA>                                                                                                  */
/*  3 c     blue    green                                                                                                 */
/*  4 d     red     <NA>                                                                                                  */
/*                                                                                                                        */
/*  WPS BASE                                                                                                              */
/*                                                                                                                        */
/*  Obs    ID    COLOR_1    COLOR_2                                                                                       */
/*                                                                                                                        */
/*   1     a      red        blue                                                                                         */
/*   2     b      green                                                                                                   */
/*   3     c      blue       green                                                                                        */
/*   4     d      red                                                                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                     _   _                             _
| ___|  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
|___ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                 |_|         |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%let _max=2;

%utl_submit_wps64x("
options validvarname=any lrecl=32756;
libname sd1 'd:/sd1';
proc python;
export data=sd1.have python=have;
submit;
print(have);
from os import path;
import pandas as pd;
import numpy as np;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
   select
      id
     ,max(case when partition=1     then color else NULL end) as color1
     ,max(case when partition=&_max then color else NULL end) as color&_max
   from
      (select id, color, row_number() OVER (PARTITION BY id) as partition from have )
   group
     by id
''');
print(want);
endsubmit;
import data=sd1.want python=want;
proc print data=sd1.want width=min;
run;quit;
");

proc print data=sd1.want width=min;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                                                                                                                        */
/* The WPS PYTHON Procedure                                                                                               */
/*                                                                                                                        */
/* A little strange                                                                                                       */
/*                                                                                                                        */
/*   id color1 color2                                                                                                     */
/* 0  a  red    blue                                                                                                      */
/* 1  b  green   None                                                                                                     */
/* 2  c  blue   green                                                                                                     */
/* 3  d  red     None                                                                                                     */
/*                                                                                                                        */
/* WPS base                                                                                                               */
/*                                                                                                                        */
/* Obs    id    color1    color2                                                                                          */
/*                                                                                                                        */
/*  1     a     red       blue                                                                                            */
/*  2     b     green     None     ==> None is a strangen mapping of sql NULLs                                            */
/*  3     c     blue      green                                                                                           */
/*  4     d     red       None                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*
 _ __ ___   __ _  ___ _ __ ___
| `_ ` _ \ / _` |/ __| `__/ _ \
| | | | | | (_| | (__| | | (_) |
|_| |_| |_|\__,_|\___|_|  \___/

*/

%macro sqlPartition(data,by=);

  (select
     row_number
    ,row_number - min(row_number) +1 as partition
    ,*
  from
      (select *, monotonic() as row_number from
         (select *, max(%scan(%str(&by),1,%str(,))) as delete from &data group by &by ))
  group
      by &by )

%mend sqlPartition;

REPO
---------------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning



/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
