from azure.storage.blob import ContainerClient , BlobClient
import sys
from sys import path
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
import pandas as pd
from azure.storage.blob import ContainerClient, BlobClient
from azure.storage.blob import BlobServiceClient
from io import StringIO, BytesIO
import datetime
import numpy as np




def clean(fname_blob):
    """
    Clean function: with an API call (Azure Function) cleans up the desired csv file.
                    it connects in the container of original files then cleans the file that you want then puts them on the container of cleaned files.
    Parameter:
     fname_blob => the name of csv file that u want to clean.
    Return:
     cleaned_ +the name of file csv

    """
    conn_str='your_connection_string'
    container_name= 'Originalfile'
    container_client = ContainerClient.from_connection_string(conn_str=conn_str, container_name=container_name)
    existing_blob_list = [blob.name for blob in container_client.list_blobs()] 

    fname_blob = f'{fname_blob}.csv'
    blob = BlobClient.from_connection_string(conn_str= conn_str,container_name='cleandfile', blob_name=f'cleaned_{fname_blob}_output.csv')
    downloaded_blob = container_client.download_blob(fname_blob)

    if fname_blob == 'cO2ConsumptionByFood.csv':

            c02_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            c02_df = c02_df.drop('Code', 1)
            c02_df = c02_df.rename(columns={'Entity':'Food','GHG emissions per kilogram (Poore & Nemecek, 2018)':'GHGEmissionsPerKg'})
            c02_df = c02_df.sort_values(by='GHGEmissionsPerKg', ascending=False)
            c02_df = c02_df.reset_index()
            c02_df = c02_df.drop('index', 1)
            C02 = c02_df.to_csv(index=False)
            blob.upload_blob(C02)

        
    elif fname_blob == 'StockVariationEUTotal.csv':

            StockVariationEu_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            StockVariationEu_df.dropna(inplace=True)
            StockVariationEu_df['Value']= StockVariationEu_df['Value'].astype(int)
            StockVariationEu_df = StockVariationEu_df.drop(['Domain','Domain Code','Element Code','Element','Unit', 'Year Code'], axis=1)
            StockVariationEu_df = StockVariationEu_df.rename(columns={'Value':'T.Qty'})
            StockVariationEu = StockVariationEu_df.to_csv(index=False)
            blob.upload_blob(StockVariationEu)

     

    elif fname_blob == 'FoodWasteByState.csv':

            FoodWaste_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            FoodWaste_df.dropna(inplace=True)
            FoodWaste_df['Value'] = FoodWaste_df['Value'].astype(int)
            FoodWaste_df = FoodWaste_df.drop(['Domain','Domain Code','Element Code','Element','Unit', 'Year Code'], axis=1)
            FoodWaste_df = FoodWaste_df.rename(columns={'Value':'T.Qty'})
            FoodWaste_df = FoodWaste_df.groupby(['Area','Year']).sum()
            FoodWaste_df  = FoodWaste_df.reset_index(level=0)
            FoodWaste_df = FoodWaste_df.reset_index(level=0)
            neworder2 = ['Area','Year','T.Qty']
            FoodWaste_df = FoodWaste_df.reindex(columns=neworder2)
            FoodWaste_df = FoodWaste_df.rename(columns={'T.Qty':'TonnesFoodWasted'})
            FoodWaste_df = FoodWaste_df.groupby(['Area']).sum()
            FoodWaste_df = FoodWaste_df.drop(['Year'], axis=1)
            FoodWaste_df = FoodWaste_df.reset_index(level=0)
            FoodWaste_df.at[17,'Area']='Bolivia'
            FoodWaste_df.at[77,'Area']='Iran'
            FoodWaste_df.at[174,'Area']='Venezuela'
            FoodWaste = FoodWaste_df.to_csv(index=False)
            blob.upload_blob(FoodWaste)

        

    elif fname_blob == 'WaterConsumptionByFood.csv':

            WaterConsumption_df= pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            WaterConsumption_df= WaterConsumption_df.drop('Code', 1)
            WaterConsumption_df = WaterConsumption_df.rename(columns={'Freshwater withdrawals per kilogram (Poore & Nemecek, 2018)':'WaterPerKg'})
            WaterConsumption = WaterConsumption_df.to_csv(index=False)
            blob.upload_blob(WaterConsumption)

    elif fname_blob == 'LandConsumptionByFood.csv':

            LandConsumption_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
            LandConsumption_df = LandConsumption_df.drop('Code', 1)
            LandConsumption_df = LandConsumption_df.rename(columns={'Land use per kilogram (Poore & Nemecek, 2018)':'LandPerKg'})
            LandConsumption = LandConsumption_df.to_csv(index = False)
            blob.upload_blob(LandConsumption)

    elif fname_blob == 'Obesity2.csv':

        ObesityPercentage = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        ObesityPercentage = ObesityPercentage.drop(['Domain','Domain Code','Element Code','Item Code','Area Code (FAO)','Element','Unit', 'Year Code','Item'], axis=1)
        ObesityPercentage.at[51,'Area']='South America'
        ObesityPercentage.at[52,'Area']='South America'
        ObesityPercentage.at[53,'Area']='South America'
        ObesityPercentage.at[54,'Area']='South America'
        ObesityPercentage.at[55,'Area']='South America'
        ObesityPercentage.at[56,'Area']='South America'
        ObesityPercentage.at[57,'Area']='South America'
        ObesityPercentage.at[58,'Area']='South America'
        ObesityPercentage.at[59,'Area']='South America'
        ObesityPercentage.at[60,'Area']='South America'
        ObesityPercentage.at[61,'Area']='South America'
        ObesityPercentage.at[62,'Area']='South America'
        ObesityPercentage.at[63,'Area']='South America'
        ObesityPercentage.at[64,'Area']='South America'
        ObesityPercentage.at[65,'Area']='South America'
        ObesityPercentage.at[66,'Area']='South America'
        ObesityPercentage.at[67,'Area']='South America'
        ObesityPercentage_df = ObesityPercentage.to_csv(index = False)
        blob.upload_blob(ObesityPercentage_df)

    elif fname_blob == 'ObesityPerState.csv':

        ObesityPerState = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        ObesityPerState = ObesityPerState.drop(['Domain','Domain Code','Area Code (FAO)','Element Code','Element','Unit','Item','Item Code', 'Year Code'], axis=1)
        ObesityPerState.dropna(inplace=True)
        ObesityPerState_df = ObesityPerState.to_csv(index=False)
        blob.upload_blob(ObesityPerState_df)

    elif fname_blob == 'Undernourish2.csv':

        UndernourishmentPercentage = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        UndernourishmentPercentage.drop(UndernourishmentPercentage[UndernourishmentPercentage['Item']== 'Prevalence of undernourishment (percent) (3-year average)'].index,inplace=True)
        UndernourishmentPercentage = UndernourishmentPercentage.drop(['Domain','Domain Code','Element Code','Item Code','Area Code (FAO)','Element','Unit', 'Year Code','Item'], axis=1)
        UndernourishmentPercentage = UndernourishmentPercentage.reset_index()
        UndernourishmentPercentage = UndernourishmentPercentage.drop('index', 1) 
        UndernourishmentPercentage.at[63,'Area']='South America'
        UndernourishmentPercentage.at[64,'Area']='South America'
        UndernourishmentPercentage.at[65,'Area']='South America'
        UndernourishmentPercentage.at[66,'Area']='South America'
        UndernourishmentPercentage.at[67,'Area']='South America'
        UndernourishmentPercentage.at[68,'Area']='South America'
        UndernourishmentPercentage.at[69,'Area']='South America'
        UndernourishmentPercentage.at[70,'Area']='South America'
        UndernourishmentPercentage.at[71,'Area']='South America'
        UndernourishmentPercentage.at[72,'Area']='South America'
        UndernourishmentPercentage.at[73,'Area']='South America'
        UndernourishmentPercentage.at[74,'Area']='South America'
        UndernourishmentPercentage.at[75,'Area']='South America'
        UndernourishmentPercentage.at[76,'Area']='South America'
        UndernourishmentPercentage.at[77,'Area']='South America'
        UndernourishmentPercentage.at[78,'Area']='South America'
        UndernourishmentPercentage.at[79,'Area']='South America'
        UndernourishmentPercentage.at[80,'Area']='South America'
        UndernourishmentPercentage.at[81,'Area']='South America'
        UndernourishmentPercentage.at[82,'Area']='South America'
        UndernourishmentPercentage.at[83,'Area']='South America'
        UndernourishmentPercentage_df = UndernourishmentPercentage.to_csv(index=False)
        blob.upload_blob(UndernourishmentPercentage_df)

    elif fname_blob == 'UndernourishPerState.csv':
        UndernourishPerState = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        UndernourishPerState = UndernourishPerState.drop(['Domain','Item','Domain Code','Area Code (FAO)','Element Code','Element','Unit','Item Code', 'Year Code'], axis=1)
        UndernourishPerState['Year'] = UndernourishPerState['Year'].str.slice(start=-4)
        UndernourishPerState['Year'] = UndernourishPerState['Year'].astype(int)-1
        UndernourishPerState.dropna(inplace=True)
        UndernourishPerState_df = UndernourishPerState.to_csv(index=False)
        blob.upload_blob(UndernourishPerState_df)

    elif fname_blob == 'GDPByState.csv':

        GDPByState = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        GDPByState = GDPByState.drop(['Domain','Domain Code','Area Code (FAO)','Element Code','Element','Item','Unit','Item Code', 'Year Code'], axis=1)
        GDPByState.dropna(inplace=True)
        GDPByState_df = GDPByState.to_csv(index=False)
        blob.upload_blob(GDPByState_df)

    elif fname_blob == 'FoodWasteNew.csv':

        FoodWaste2_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        FoodWaste2_df = FoodWaste2_df.drop(['Domain','Domain Code','Area','Area Code (FAO)','Element Code','Element','Item Code (CPC)','Unit', 'Year Code'], axis=1)
        FoodWaste2_df = FoodWaste2_df.groupby('Item').sum()
        FoodWaste2_df = FoodWaste2_df.reset_index(level=0)
        FoodWaste2_df = FoodWaste2_df.drop(['Year'], axis=1)
        FoodWaste2_df = FoodWaste2_df.rename(columns={'Value':'WastedTonnes'})
        FoodWaste2_df = FoodWaste2_df[FoodWaste2_df.WastedTonnes != 0.0]
        FoodWaste2_df['WastedTonnes']= FoodWaste2_df['WastedTonnes'].astype(int)
        FoodWaste2 = FoodWaste2_df.to_csv(index=False)
        blob.upload_blob(FoodWaste2)

    elif fname_blob == 'ContinentAndStates.csv':

        Continent_df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        Continent_df = Continent_df.drop(['Country Group Code','Country Code','M49 Code','ISO2 Code','ISO3 Code'], axis=1)
        Continent_df = Continent_df.rename(columns={'Country Group':'Continent'})
        Continent_df.drop(Continent_df[Continent_df['Continent']=='European Union (27)'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Land Locked Developing Countries'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Least Developed Countries'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Low Income Food Deficit Countries'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Melanesia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Micronesia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Middle Africa'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Net Food Importing Developing Countries'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Northern Africa'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Northern Europe'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Polynesia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Small Island Developing States'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='South-eastern Asia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Southern Africa'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Southern Europe'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Southern Asia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Western Africa'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Western Asia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Western Europe'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='World'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Eastern Africa'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Americas'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Eastern Europe'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Eastern Asia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Central Asia'].index,inplace=True)
        Continent_df.drop(Continent_df[Continent_df['Continent']=='Australia and New Zealand'].index,inplace=True)
        Continent_df.at[132,'Continent']='South America'
        Continent_df.at[133,'Continent']='South America'
        Continent_df.at[134,'Continent']='South America'
        Continent_df.at[135,'Continent']='South America'
        Continent_df.at[136,'Continent']='South America'
        Continent_df.at[137,'Continent']='South America'
        Continent_df.at[138,'Continent']='South America'
        Continent_df.at[139,'Continent']='South America'
        Continent_df.at[140,'Continent']='South America'
        Continent_df.at[141,'Continent']='South America'
        Continent_df.at[142,'Continent']='South America'
        Continent_df.at[143,'Continent']='South America'
        Continent_df.at[144,'Continent']='South America'
        Continent_df.at[145,'Continent']='Northern America'
        Continent_df.at[146,'Continent']='Northern America'
        Continent_df.at[147,'Continent']='Northern America'
        Continent_df.at[148,'Continent']='Northern America'
        Continent_df.at[149,'Continent']='Northern America'
        Continent_df.at[150,'Continent']='Northern America'
        Continent_df.at[151,'Continent']='Northern America'
        Continent_df.at[152,'Continent']='Northern America'
        Continent_df = Continent_df.reset_index()
        Continent_df = Continent_df.drop('index', 1)
        Continent_df.at[168,'Country']='Bolivia'
        Continent_df.at[64,'Country']='Iran'
        Continent_df.at[178,'Country']='Venezuela'
        Continent = Continent_df.to_csv(index=False)
        blob.upload_blob(Continent)

    elif fname_blob == 'FOFA2050CountryData_Market.csv':
        foodprojection = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
        foodprojection = foodprojection.drop(['Domain','Indicator','Element','CountryCode','Region', 'Units'], axis = 1)
        foodprojection.drop(foodprojection[foodprojection['Scenario']=='Toward Sustainability'].index,inplace=True)
        foodprojection.drop(foodprojection[foodprojection['Scenario']=='Stratified Societies'].index,inplace=True)
        foodprojection = foodprojection.groupby(['Item','Year']).sum()
        foodprojection = foodprojection.reset_index(level=0)
        foodprojection = foodprojection.reset_index(level=0)
        neworder = ('Item','Year','Value')
        foodprojection = foodprojection.reindex(columns=neworder)
        foodprojection_df = foodprojection.to_csv(index=False)
        blob.upload_blob(foodprojection_df)
        
        return("cleaned_" +fname_blob)

    else:
            return( f'Atttention, {fname_blob} not exists, the existing files are:{existing_blob_list}')










