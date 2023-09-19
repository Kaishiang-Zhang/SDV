#%%
from anonymeter.evaluators import SinglingOutEvaluator
from anonymeter.evaluators import LinkabilityEvaluator
from anonymeter.evaluators import InferenceEvaluator
import matplotlib.pyplot as plt
import pandas as pd
import glob

def detect1():
    #單獨識別
    evaluator = SinglingOutEvaluator(ori=originalData, 
                                syn=syntheticData, 
                                control=controlData,
                                n_attacks=250)

    try:
        evaluator.evaluate(mode='univariate')
        risk = evaluator.risk()
        # print(risk)

    except RuntimeError as ex: 
        print(f"單獨識別評估失敗，原因是 {ex}。請重新運行此單元格。"
          "為獲得更穩定的結果，增加`n_attacks`。請注意，這將使評估速度變慢。")
    
    # print("攻擊者在單獨識別攻擊中猜測的資料：{}".format(evaluator.queries()[:3]))

    #　對隱私風險的估計，以及其置信區間。
    evaluator.risk(confidence_level=0.95)

    res = evaluator.results()
    # "主要"隱私攻擊，攻擊者使用合成數據來猜測原始數據中的信息。
    # "基線"攻擊，模擬一個忽略合成數據並隨機猜測的天真攻擊者。
    # "控制"隱私攻擊，攻擊者使用合成數據來猜測控制數據集中的信息。

    # print("主要攻擊的成功率:", res.attack_rate)
    # print("基線攻擊的成功率:", res.baseline_rate)
    # print("控制攻擊的成功率:", res.control_rate)
    res.risk()
    avgRisk = (res.attack_rate.value + res.baseline_rate.value + res.control_rate.value)/3
    return avgRisk


    

     

def detect2():
    # 結合不同屬性的預測來對數據集進行攻擊。稱為多變量預測。
    evaluator = SinglingOutEvaluator(ori=originalData, 
                                    syn=syntheticData, 
                                    control=controlData, 
                                    n_attacks=100, # this attack takes longer
                                    n_cols=4)


    try:
        evaluator.evaluate(mode='multivariate')
        risk = evaluator.risk()
        # print(risk)

    except RuntimeError as ex: 
        print(f"單獨識別評估失敗，原因是 {ex}。請重新運行此單元格。"
          "為獲得更穩定的結果，增加`n_attacks`。請注意，這將使評估速度變慢。")
    
    # print("攻擊者在單獨識別攻擊中猜測的資料：{}}".format(evaluator.queries()[:3]))    
    res = evaluator.results()
    # print("主要攻擊的成功率:", res.attack_rate)
    # print("基線攻擊的成功率:", res.baseline_rate)
    # print("控制攻擊的成功率:", res.control_rate)
    avgRisk = (res.attack_rate.value + res.baseline_rate.value + res.control_rate.value)/3
    return avgRisk

def detect3():

    aux_cols = [
            ['編號', '時間', '區', '村里', '案件類型'],
            [ '村里代碼', '年齡區間', '性別', '總計']
        ]
    # 基於某些屬性子集來連結兩個其他數據集的隱私風險。 n_neighbors參數，可將連結性做調整。
    evaluator = LinkabilityEvaluator(ori=originalData, 
                                    syn=syntheticData, 
                                    control=controlData,
                                    n_attacks=500,
                                    aux_cols=aux_cols,
                                    n_neighbors=10)

    evaluator.evaluate(n_jobs=-2)  # n_jobs follow joblib convention. -1 = all cores, -2 = all execept one
    evaluator.risk()
    res = evaluator.results()
    
    # print("主要攻擊的成功率:", res.attack_rate)
    # print("基線攻擊的成功率:", res.baseline_rate)
    # print("控制攻擊的成功率:", res.control_rate)
    avgRisk = (res.attack_rate.value + res.baseline_rate.value + res.control_rate.value)/3
    return avgRisk
    # 調整n_neighbors，將連結性降低。
    # print(evaluator.risk(n_neighbors=7))

def detect4():

    columns = originalData.columns
    results = []
    # print(columns)
    risk = 0
    num = 0 
    for secret in columns:
        
        aux_cols = [col for col in columns if col != secret]

        # print(aux_cols)
        # 測量推斷風險
        evaluator = InferenceEvaluator(ori=originalData, 
                                    syn=syntheticData, 
                                    control=controlData,
                                    aux_cols=aux_cols,
                                    secret=secret,
                                    n_attacks=500)
        evaluator.evaluate(n_jobs=-2)
        results.append((secret, evaluator.results()))
        evaluator.risk()
        res = evaluator.results()
        
        # print("主要攻擊的成功率:", res.attack_rate)
        # print("基線攻擊的成功率:", res.baseline_rate)
        # print("控制攻擊的成功率:", res.control_rate)
        risk = risk +(res.attack_rate.value + res.baseline_rate.value + res.control_rate.value)/3
        num +=1
    return(risk/num)
        # fig, ax = plt.subplots()

        # risks = [res[1].risk().value for res in results]
        # columns = [res[0] for res in results]

        # ax.bar(x=columns, height=risks, alpha=0.5, ecolor='black', capsize=10)

        # plt.xticks(rotation=45, ha='right')
        # ax.set_ylabel("Measured inference risk")
        # _ = ax.set_xlabel("Secret column")
 


        


if __name__ == '__main__':

    originalData = pd.read_csv(glob.glob('Original_data/' +'*.csv')[0])
    controlData = pd.read_csv(glob.glob('Control_data/' +'*.csv')[0])
    syntheticDataList = glob.glob('Synthetic_data/'+'*.csv')
    # syntheticData = pd.read_csv('Synthetic_data/{}'.format('Synthetic_CTGAN_1.csv'))
    riskDict = {}
    for fileName in syntheticDataList:
        totalrisk = 0.0
        totalnum = 0
        syntheticData = pd.read_csv(fileName)
        totalrisk = totalrisk + (detect1() + detect2() + detect3() + detect4())/4
        totalnum +=1
        riskDict[fileName] = totalrisk/totalnum
    for filename in riskDict.keys():
        print("{}隱私風險：{}".format(filename,riskDict[filename]))
    # print("FileName:{}隱私風險：{}".format(fileName,totalrisk/totalnum))
        
    # syntheticData = pd.read_csv(syntheticDataList[0])
    # detect1()



        
        
# %%
