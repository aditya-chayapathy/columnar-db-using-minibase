# Columnar DB Implementation using Minibase

This project involves using a relational DBMS (i.e. Minibase) as a backbone to develop a full-fledged columnar DBMS. Several query operators are built on top of this columnar DBMS and they are described below:

Types of Index Supported: 
  1. BITMAP  
  2. BTREE

Types of Scan Supported:
  1. FILESCAN
  2. COLUMNSCAN
  3. BITMAP SCAN
  4. BTREE SCAN

Type of query expression supported:
  1. CNF

Intefaces:
- Batch Insert:<br />
  Interface: BatchInsert<br />
  Usage: java BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS<br />
  Example: java BatchInsert sampledata.txt testColumnDB students 4<br />
  
- Create Index:<br />
  Interface: CreateIndex<br />
  Usage: java CreateIndex COLUMNDBNAME COLUMNARFILENAME COLUMNNAME TYPEOFINDEX<br />
  Example: java CreateIndex testColumnDB students C BITMAP<br />

- Select Query:   <br />
  Interface: SelectQuery<br />
  Usage: java SelectQuery COLUMNDBNAME COLUMNARFILENAME PROJECTION OTHERCONSTRAINTS COLUMNSCANS SCANTYPES SCANCONSTRAINTS TARGETCOLUMNS NUMBUF SORTMEM<br />
  Example: java SelectQuery DemoDB DemoFile A,C,D "" C,D BITMAP,BTREE "(C = 5 v C = 9),D >= 5" A,C,D 200 100<br />

- Delete Query:<br />
  Interface: DeleteQuery<br />
  Usage: DeleteQuery COLUMNDBNAME COLUMNARFILENAME PROJECTION OTHERCONSTRAINTS COLUMNSCANS SCANTYPES SCANCONSTRAINTS TARGETCOLUMNS NUMBUF SORTMEM<br />
  Example: java DeleteQuery DemoDB DemoFile A,C,D "" C,D BITMAP,BTREE "(C = 5 v C = 9),D >= 5" A,C,D 200 100 <br />
  
- Bitmap Equijoin:<br />
  Interface:<br />
  Usage: <br />
  Example: <br />
