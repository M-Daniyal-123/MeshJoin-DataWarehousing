# MeshJoin-DataWarehousing
Real time Data Warehousing using state of the art real time semi-stream join algorithm MeshJoin. The code uploaded uses MeshJoin, a semi-stream join algorithm, based on [R-MeshJoin](https://dl.acm.org/doi/10.1145/1871940.1871952) paper.
The repository contains a Java implementation of a real time DataWarehousing. The program mimics real transactions as stream by reading synthetic transactions from a MySQL database, and performs enrichment of the streams by applying MeshJoin as join algorithm in the ETL layer. 
For the DataModel the program uses a Star Schema structure (All queries are in createDWH.sql), and also there are some sample analytical queries in queries.sql. 


For gaining access to synthetic dataset you can email at: daniyalshaiq@gmail.com
