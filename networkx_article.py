#pip install EIA-python
#pip install networkx
import numpy as np 
import pandas as pd
import eia
import networkx as nx
import matplotlib.pyplot as plt

#Get API key from EIA website and pass into eia.API() method
apiKey = "5f54b3e66477e22ec068066b1de8026d"
api = eia.API(apiKey)

series_id_list = ["INTL.57-1-DZA-TBPD.M", "INTL.57-1-AGO-TBPD.M", "INTL.57-1-COG-TBPD.M", "INTL.57-1-COD-TBPD.M", 
                  "INTL.57-1-ECU-TBPD.M","INTL.57-1-GNQ-TBPD.M","INTL.57-1-GAB-TBPD.M","INTL.57-1-IRN-TBPD.M",
                  "INTL.57-1-IRQ-TBPD.M", "INTL.57-1-KWT-TBPD.M", "INTL.57-1-LBY-TBPD.M", "INTL.57-1-NGA-TBPD.M",
                  "INTL.57-1-QAT-TBPD.M", "INTL.57-1-RUS-TBPD.M", "INTL.57-1-SAU-TBPD.M","INTL.57-1-ARE-TBPD.M",
                  "INTL.57-1-VEN-TBPD.M", "INTL.57-1-USA-TBPD.M"]

#call the method for each series within the api.data_by_series() method and plug into a pandas dataframe
df_list = [pd.DataFrame(api.data_by_series(series)) for series in series_id_list]
oil_data = pd.concat(df_list, axis=1)

#Drop NAN values
oil_data = oil_data.replace("--", np.nan)
oil_data_reduced = oil_data.dropna()
oil_data_reduced

#Rename Columns
oil_data_reduced.columns = ["Algeria", "Angola", "Congo-Brazzaville", "Congo-Kinshasa", "Ecuador", "Equatorial Guinea", "Gabon", "Iran",
                "Iraq", "Kuwait", "Libya", "Nigeria", "Qatar", "Russia", "Saudi Arabia", "United Arab Emirates", "Venezuela", "USA"]

#Check out the highest oil producing countries by average volume
oil_data_reduced.mean(axis=0).sort_values(ascending=False)

#get correlations
corrs = oil_data_reduced.corr()

 
# Transform it in a links data frame (3 columns only):
links = corrs.stack().reset_index()
links

 
links.columns = ['country A', 'country B','value']

# Positive Correlations
# Keep only correlation over a threshold and remove self correlation (cor(A,A)=1)
positive_correlations =links.loc[ (links['value'] >= 0.5) & (links['country A'] != links['country B']) ]

positive_links = nx.from_pandas_edgelist(positive_correlations, 'country A', 'country B')
pos = nx.circular_layout(positive_links)

labels = list(pos.keys())
labels = dict(zip(labels, labels))

pos_higher = {}
for country, value in pos.items():
    if(value[1]>0):
        pos_higher[country] = (value[0], value[1]+0.06)
    else:
        pos_higher[country] = (value[0], value[1]-0.06)
        
pos_higher['Congo-Kinshasa'] = (pos_higher['Congo-Kinshasa'][0]+.06, pos_higher['Congo-Kinshasa'][1])
pos_higher['Algeria'] = (pos_higher['Algeria'][0]+.05, pos_higher['Algeria'][1]+.1)
pos_higher['Saudi Arabia'] = (pos_higher['Saudi Arabia'][0]-.04, pos_higher['Saudi Arabia'][1])
pos_higher['Qatar'] = (pos_higher['Qatar'][0]-.03, pos_higher['Qatar'][1])
pos_higher['Kuwait'] = (pos_higher['Kuwait'][0]-.03, pos_higher['Kuwait'][1])
pos_higher['Nigeria'] = (pos_higher['Nigeria'][0]+.01, pos_higher['Nigeria'][1])
pos_higher['Angola'] = (pos_higher['Angola'][0]+.01, pos_higher['Angola'][1])


fig, ax = plt.subplots(figsize=(20,20))
margin=.05
fig.subplots_adjust(margin, margin, 1.-margin, 1.-margin)
ax.axis('equal')
nx.draw(positive_links, pos=pos, edge_color='black')
nx.draw_networkx_labels(positive_links, pos_higher,labels)
plt.savefig("positive_correlation.jpg")

#Negative Correlations
 
negative_correlations =links.loc[ (links['value'] <= -0.5) & (links['country A'] != links['country B']) ]
negative_links = nx.from_pandas_edgelist(negative_correlations, 'country A', 'country B')
neg = nx.circular_layout(negative_links)

labels = list(neg.keys())
labels = dict(zip(labels, labels))

neg_higher = {}

for country, value in neg.items():
    if(value[1]>0):
        neg_higher[country] = (value[0], value[1]+0.06)
    else:
        neg_higher[country] = (value[0], value[1]-0.06)
        
neg_higher['Congo-Kinshasa'] = (neg_higher['Congo-Kinshasa'][0]+.05, neg_higher['Congo-Kinshasa'][1])
neg_higher['Algeria'] = (neg_higher['Algeria'][0]+.05, neg_higher['Algeria'][1]+.1)


fig, ax = plt.subplots(figsize=(20,20))
margin=.05
fig.subplots_adjust(margin, margin, 1.-margin, 1.-margin)
ax.axis('equal')
nx.draw(negative_links, pos=neg, edge_color='black')
nx.draw_networkx_labels(negative_links, neg_higher,labels)
plt.savefig("negative_correlation.jpg")

#Calculating Social Graph Measurements

#Positive Correlations: We can only calculate the shortest_path_length because a disconnected graph exists.
#Eccentricity and diameter would throw errors due to the disconnected graph.
list(nx.shortest_path_length(positive_links))
positive_correlation['country A'].value_counts()

#Negative Correlations
eccentricity = nx.eccentricity(negative_links)
ecc_df = pd.DataFrame.from_dict(eccentricity, orient='index', columns=['Eccentricity']).rename_axis('Country', axis='index')
ecc_df.sort_values(by='Eccentricity', ascending=False)
nx.diameter(negative_links, e=eccentricity)
list(nx.shortest_path_length(negative_links))
