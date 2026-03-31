# qs-hub
Quantified self data integration and analysis tools.


# Plan
* Get this example pipeline working and merged 
* Sort idempotency - example.csv and example2.csv - importing example.csv twice should - create a new timestampted file in bronze, but the import process to silver should have no effect. example2.csv will have the same as example, but one extra row. Just that extra row should be imported. 
* Then in my fork I create the real pipeline for the daily tracker 
* Then finally get to practicing making machine learning models