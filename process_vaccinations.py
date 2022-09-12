import os
from pathlib import Path
from typing import Union, Iterable

import pandas as pd

from storage.connection import DataStorage
from handler.import_file import FileImporter

populations_path = Path(os.curdir) / 'data' / 'country_populations.csv'
vaccinations_path = Path(os.curdir) / 'data' / 'vaccinations.csv'
default_year = "2020"

db_connection = DataStorage("zadacha.db")


class ProcessVaccinations:

    def __init__(self):
        self.populations_df = FileImporter(populations_path).read_csv(concat=True)
        self.vaccinations_df = FileImporter(vaccinations_path).read_csv(concat=True)

    def _filter_population(self, year: Union[str, Iterable[str]] = default_year) -> pd.DataFrame:
        """
           Get from the country population data the ones that do not
           start from OWID and correspond the requested year
           :param year: str or List of str
           Years that needs to be returned
           :return: DataFrame
           Filtered dataframe
           """
        if isinstance(year, str):
            required_fields = ['Country Code', year]
        elif isinstance(year, Iterable):
            required_fields = ['Country Code', *year]
        else:
            raise TypeError("Wrong year variable type!")

        df_mask = ~self.populations_df.loc[:, 'Country Code'].str.startswith("OWID_")

        return self.populations_df.loc[df_mask, required_fields]

    def _get_latest_vaccination(self) -> pd.DataFrame:
        grouped = self.vaccinations_df.groupby("iso_code")["date"].max().reset_index()
        grouped_renamed = grouped.rename(columns={"date": "max_date"})
        self.vaccinations_df = pd.merge(self.vaccinations_df, grouped_renamed, how="left", on=["iso_code"])

        df_mask = self.vaccinations_df['date'] == self.vaccinations_df['max_date']

        return self.vaccinations_df.loc[df_mask].reset_index(drop=True)

    @staticmethod
    def _upsert_countries_table(row: pd.Series):
        sql = f"""
            SELECT * FROM countries c
            WHERE c.iso_code = '{row["iso_code"]}'
            """
        country_data = db_connection.execute(sql)

        insert_data = (row["location"],
                       row["iso_code"],
                       row["population"],
                       row["people_fully_vaccinated"],
                       row["percentage_vaccinated"])
        insert_sql = f"""
                INSERT INTO countries
               (name, iso_code, population, total_vaccinated, percentage_vaccinated) 
               VALUES (?, ?, ?, ?, ?)            
            """

        update_data = (row["population"],
                       row["people_fully_vaccinated"],
                       row["percentage_vaccinated"],
                       row["iso_code"])
        update_sql = f"""
                UPDATE countries SET 
                population = ?,
                total_vaccinated = ?,
                percentage_vaccinated = ?
                WHERE iso_code = ?
            """

        upsert_sql = update_sql if country_data else insert_sql
        upsert_data = update_data if country_data else insert_data
        db_connection.execute(upsert_sql, values=upsert_data, commit=True)

    def process(self, year: str = default_year):
        """
        Merge the vaccination data with filtered population data and calculate the percentage of vaccination
        :return:
        """
        vaccinations = self._get_latest_vaccination()
        print("PRC: Get vaccinations")
        filtered_populations = self._filter_population()
        print("PRC: Filter populations")

        dataframe = pd.merge(vaccinations, filtered_populations, left_on=["iso_code"], right_on=["Country Code"])
        dataframe = dataframe.fillna(0)
        dataframe['percentage_vaccinated'] = 100 * dataframe['people_fully_vaccinated'] / dataframe[year]
        dataframe = dataframe.rename(columns={year: 'population'})
        print("PRC: Dataframe is prepared")
        print("PRC: Updating db...")
        for _id, row in dataframe.iterrows():
            self._upsert_countries_table(row)


if __name__ == "__main__":
    ProcessVaccinations().process()
