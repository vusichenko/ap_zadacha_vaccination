The dataStorage class was created to connect to the database and perform basic operations, the future can be extended for more specific tasks.

The FileImporter class was created, capable of loading large .csv files into pandas Dataframe for further data processing.

And there is a basic narrowly focused ProcessVaccinations class for processing and filtering data from the file and writing it to the database.


If your table in the database has not yet been prepared, you need to create a zadacha.db file and run
python prepare.py

To start processing vaccinations, run process_vaccinations.py