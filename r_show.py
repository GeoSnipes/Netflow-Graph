import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
from mpl_toolkits.mplot3d import Axes3D
import statsmodels.formula.api as sm
import numpy as np

finalOutput = [
    # "netflowFinalised/igate.201802010005.csv",
    # "netflowFinalised/igate.201802010010.csv",
    # "netflowFinalised/igate.201802010015.csv",
    # "netflowFinalised/igate.201802010020.csv",
    # "netflowFinalised/igate.201802010025.csv",
    # "netflowFinalised/igate.201802010030.csv",
    # "netflowFinalised/igate.201802010035.csv",
    # "netflowFinalised/igate.201802010040.csv",
    # "netflowFinalised/igate.201802010045.csv",
    # "netflowFinalised/igate.201802010050.csv"
]

data = pd.read_csv("netflowFinalised/igate.201802010000editfinal.csv", sep=',', header=0)  # load csv file
for list in finalOutput:
    data.append(pd.read_csv(list, sep=',', header=0))
# summary = data.describe()  # Calculate summary statistics
# summary = summary.transpose()  # Transpose statistics to get similar format as R summary() function
# print(summary)  # Visualize summary statistics in console
    # print(data.head())
data.sort_values('EpochTime', ascending=True)

# result = sm.ols(formula="Bytes ~ Protocol + DstPort + Tos", data=data).fit()
# result = sm.ols(formula="Bytes ~ Duration + Proto +SrcPort+ DstPort + Tos", data=data).fit()
# resultOLS = sm.ols(formula="Bytes ~ Proto + SrcPort + DstPort + Tos", data=data).fit()
# resultOLS = sm.ols(formula="Bytes ~ Src", data=data).fit()
    # resultOLS = sm.ols(formula="Packets ~ Duration", data=data).fit()
    # print(resultOLS.params)
    # print(resultOLS.summary())

# resultTS = sm.tsa(formula="Bytes ~ Proto + SrcPort + DstPort + Tos", data=data).fit()
# print(resultOLS.params)
# print(resultOLS.summary())


# plt.hist(data['Duration'], bins=20)
# plt.show()
# plt.clf()
# plt.hist(data['Bytes'], bins=200)
# plt.show()
# plt.clf()
if True:
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(data['Dst'], data['Src'], data['Bytes'])

    ax.set_xlabel('Dst')
    ax.set_ylabel('Src')
    ax.set_zlabel('Bytes')
    plt.show()
    plt.clf()

    plt.scatter(data['Dst'], data['Bytes'])
    plt.show()
    plt.clf()

    plt.scatter(data['EpochTime'], data['Bytes'])
    plt.show()

plt.scatter(data['Duration'], data['Bytes'])
plt.xlabel('Duration')
plt.ylabel('Bytes')
plt.show()
