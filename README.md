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
  Interface: BitmapEquiJoin<br />
  Usage: java BitmapEquiJoin COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST TARGETCOLUMNS NUMBUF <br />
  Example: java BitmapEquiJoin test R1 R2 "([R1.B = 'West_Virginia'])" "([R2.A = 'South_Dakota'])" "([R1.C = R2.C]) ^ ([R1.A = R2.A])" R1.A,R1.B,R1.C,R1.D,R2.A,R2.B,R2.C,R2.D 125<br />
  
- Columnar Nested Loop Join: <br />
  Interface: ColumnarNestedLoopJoin<br />
  Usage: java ColumnarNestedLoopJoin COLUMNDB OUTERFILE INNERFILE PROJECTION OUTERCONST OUTERSCANCOLS OUTERSCANTYPE OUTERSCANCONST OUTERTARGETCOLUMNS INNERCONST INNERTARGETCOLUMNS JOINCONDITION NUMBUF<br />
  Example: java ColumnarNestedLoopJoin DemoDB DemoFile DemoFile2 DemoFile.A,DemoFile.C,DemoFile2.C,DemoFile2.A,DemoFile2.C "DemoFile.D = 0" A,C BITMAP,BITMAP "(DemoFile.A = 'Kansas',DemoFile.C < 9)" A,B,C,D "(DemoFile2.C > 5)" A,B,C,D "DemoFile.A = DemoFile2.A ^ DemoFile.C = DemoFile.C" 200 100<br />
  
- Sort: <br />
  Interface: Sort<br />
  Usage: java Sort COLUMNDB COLUMNARFILE PROJECTION OTHERCONST SCANCOLS SCANTYPE TARGETCOLUMNS SORTORDER NUMBUF NUMPAGE SORTMEM<br />
  Example: java Sort DemoDB DemoFile A,B,C,D "" "" FILE "" A,B,C,D A ASC 100 3 100<br />
