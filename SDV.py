#%%
from sdv.datasets.local import load_csvs
from sdv.evaluation.single_table import evaluate_quality
from sdv.metadata import MultiTableMetadata
from sdv.metadata import SingleTableMetadata
from sdv.multi_table import HMASynthesizer
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.single_table import CTGANSynthesizer

from anonymeter.evaluators import SinglingOutEvaluator
from anonymeter.evaluators import LinkabilityEvaluator
from anonymeter.evaluators import InferenceEvaluator
import matplotlib.pyplot as plt

import pandas as pd
import argparse



# 報告分析API
def print_quality_report(method,datasets,synthetic_data,metadata,num):
    quality_report =evaluate_quality(
        datasets,
        synthetic_data,
        metadata
    )
    #比較最佳品質資料
    if quality_report.get_score() > bestSynthethicData['Overall Score']:
        update_best_situation(method,metadata,quality_report,num)

    
# 變更欄位資料類別
def set_multi_column_sdtype(metadata,tableName,columnName,sdtype):
    metadata.update_column(
        table_name = tableName,
        column_name = columnName,
        sdtype = sdtype
    )
def set_single_column_sdtype(metadata,columnName,sdtype):
    metadata.update_column(
        column_name = columnName,
        sdtype = sdtype
    )

# 取得使用者輸入primary key的欄位名稱
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--primarykey','--pk',help='ID or PII column',default='編號',type=str,required=False)
    parser.add_argument('--sdtype','--t',help='sdtype of primary column',default='id',type=str,required=False)
    return parser.parse_args() 

#取得檔案名稱
def get_csv_filename(datasetKey):
    return str(datasetKey).split("'")[1]

def set_report_metadata(metadata,primaryKey,csvName):
    metadataDict = metadata.to_dict()
    reportMetadata = SingleTableMetadata.load_from_dict({"primary_key":primaryKey,"columns":metadataDict['tables'][csvName]['columns']})
    return reportMetadata

#取得所有欄位可能性之資料型態
def get_columns_name(metadata,csvName):
    metadataDict = metadata.to_dict()
    columnlist = list(metadataDict['tables'][csvName]['columns'].keys())
    columnlist.remove(args.primarykey)
    return columnlist

#取得所有欄位可能2的之資料型態
def get_total_Sdtypelist(meatadata,columnName,primaryKey):
    sdtypeDict = {}
    meatadata = meatadata.to_dict()
    for column in columnName:
        if column != primaryKey:
            if meatadata['columns'][column]["sdtype"] != 'categorical':
                sdtypeDict[column] = [meatadata['columns'][column]["sdtype"],'categorical']
            else:
                sdtypeDict[column] = 'categorical'
    return  sdtypeDict

#更新最佳生成資料資訊
def update_best_situation(method,metadata,report,num):
    properties = report.get_properties()
    bestSynthethicData['Method'] = method 
    bestSynthethicData['Overall Score'] = report.get_score()
    bestSynthethicData[properties.loc[0]["Property"]] = properties.loc[0]["Score"]
    bestSynthethicData[properties.loc[1]["Property"]] = properties.loc[1]["Score"]
    bestSynthethicData['Metadata'] = metadata.to_dict()
    bestSynthethicData['File Name'] = "Synthetic_{}_{}.csv".format(method,num)





def main():
    datasets = load_csvs(folder_name='Original_Data/')
    csvName = get_csv_filename(datasets.keys())

    # 觀察載入之資料集
    data_table = datasets[csvName]
    data_table.head(5)

    # 建構生成資料所需metadata
    multiMetadata = MultiTableMetadata()
    singleMetadata = SingleTableMetadata()
    multiMetadata.detect_from_dataframes(data=datasets)
    # multiMetadata.detect_from_csvs('dataset/'+csvName+'.csv')
    singleMetadata.detect_from_csv('Original_Data/'+csvName+'.csv')
    # 產出資料類別確認圖
    # print('Auto detected data:\n')
    # metadata.visualize()

    # 設定可成為主識別符欄位之資料類別
    multiMetadata.update_column(
        table_name = csvName,
        column_name = args.primarykey,
        sdtype = args.sdtype,
        regex_format='ID_[0-9]{3,4}'
    )
    singleMetadata.update_column(
        column_name = args.primarykey,
        sdtype = args.sdtype,
        regex_format='ID_[0-9]{3,4}'
    )

    # 設定主識別符
    multiMetadata.set_primary_key(
        table_name = csvName,
        column_name = args.primarykey
    )
    singleMetadata.set_primary_key(
        column_name = args.primarykey
    )


    # 將資料轉為dataframe格式，以便分析報告API使用
    data_list = [datasets[csvName]]
    data_df=pd.DataFrame(data_list[0])

    #取得所有欄位名稱
    columnName = get_columns_name(multiMetadata,csvName)
    totalSdtypelist = get_total_Sdtypelist(singleMetadata,columnName,args.primarykey)
    for method in synDataMethods:
        num = 1
        for column in columnName:
            if totalSdtypelist[column] != 'categorical':
                for sdtype in totalSdtypelist[column]:
                    if method =='Gaussian':
                        # reportMetadata = set_report_metadata(sdtype) #此方法須先設定metadata
                        set_single_column_sdtype(singleMetadata,'總計',sdtype)
                        synthesizer = GaussianCopulaSynthesizer(singleMetadata)
                        synthesizer.fit(data_df)
                        synthetic_data = synthesizer.sample(num_rows=synDataTotal)
                        # synthetic_data.head() # 觀察新生成資料集

                        print("--------方法為{}總計欄位類別為{}時資料品質--------".format(method,sdtype))
                        print_quality_report(method,data_df,synthetic_data,singleMetadata,num)
                        synthetic_data.to_csv("Synthetic_Data/Synthetic_{}_{}.csv".format(method,num),mode='w',encoding='utf-8-sig')
                        num+=1
                    elif method =='CTGAN':
                        # reportMetadata = set_report_metadata(sdtype) #此方法須先設定metadata
                        set_single_column_sdtype(singleMetadata,'總計',sdtype)
                        synthesizer = CTGANSynthesizer(singleMetadata)
                        synthesizer.fit(data_df)
                        synthetic_data = synthesizer.sample(num_rows=synDataTotal)
                        # synthetic_data.head() # 觀察新生成資料集

                        print("--------方法為{}總計欄位類別為{}時資料品質--------".format(method,sdtype))
                        print_quality_report(method,data_df,synthetic_data,singleMetadata,num)
                        synthetic_data.to_csv("Synthetic_Data/Synthetic_{}_{}.csv".format(method,num),mode='w',encoding='utf-8-sig')
                        num+=1
                    elif method == 'HMA':
                        set_multi_column_sdtype(multiMetadata,csvName,'總計',sdtype)
                        synthesizer = HMASynthesizer(multiMetadata)
                        synthesizer.fit(datasets)

                        synthetic_data = synthesizer.sample(scale=HMAScale)
                        # synthetic_data['TestDataset'].head(5) # 觀察新生成資料集

                        syn_list = [synthetic_data[csvName]]
                        syn_df=pd.DataFrame(syn_list[0])

                        # reportMetadata = set_report_metadata(sdtype)
                        # 產生分析結果
                        print("--------方法為{}總計欄位類別為{}時資料品質--------".format(method,sdtype))
                        reportMetadata = set_report_metadata(multiMetadata,args.primarykey,csvName)
                        get_columns_name(multiMetadata,csvName)
                        print_quality_report(method,data_df,syn_df,reportMetadata,num)
                        syn_df.to_csv("Synthetic_Data/Synthetic_{}_{}.csv".format(method,num),mode='w',encoding='utf-8-sig')
                        num+=1
    for key in bestSynthethicData.keys():
        if key != 'Metadata':
            print('{} : {}'.format(key,bestSynthethicData[key]))
        else:
            for column in bestSynthethicData[key]["columns"]:
                print('{} : {}'.format(column,bestSynthethicData[key]["columns"][column]))
    
    



    # 產生視覺化資料分析圖
    # quality_report.get_visualization('Column Shapes')
    # for name in columnName:
    #     fig = get_column_plot(
    #         real_data=data_df,
    #         synthetic_data=syn_df,
    #         column_name=name,
    #         metadata=metadata
    #     )

    #     fig.show()



if __name__ == '__main__':
    args = get_args()
    # 參數設定  
    csvName = '' #檔案名稱
    num = 0 #匯出csv檔名名稱用
    HMAScale = 2 # HMA方法生成的新資料倍數
    synDataTotal = 1000 #高斯及CTGAN所需，欲生成資料筆數
    stringSdtype = 'categorical'
    synDataMethods = ['Gaussian','CTGAN','HMA']
    # synDataMethods = []
    bestSynthethicData = {"Method":"","Overall Score":0,"Column Shapes":0,"Column Pair Trends":0,"Metadata":"","File Name":""}
    totalSdtypelist = {} #宣告一個空字典用來放置所有欄位可能之資料型態

    # columnName = ['時間','區','村里','村里代碼','年齡區間','性別','案件類型','總計']
    # totalSdtypelist = ['numerical','categorical'] # "總計"欄位之資料類型可能性()
    main()
    








# %%
