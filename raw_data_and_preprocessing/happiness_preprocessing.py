import boto3
import pandas as pd
import numpy as np
import io

# set up client for s3
client = boto3.client('s3')


#****** 2015 data ******#
obj = client.get_object(Bucket='edu.gwu.ds.cc.g49487749',Key='2015.csv')
data = obj['Body'].read().decode('utf-8')
df_15 = pd.read_csv(io.StringIO(data), low_memory=False)
df_15.head()


df_15.drop(['Dystopia Residual','Standard Error'], axis=1, inplace=True)


df_15.rename(columns={'Happiness Rank':'Rank',
                      'Happiness Score':'Score',
                      'Economy (GDP per Capita)':'GDPperCapita',
                      'Family':'SocialSupport',
                      'Health (Life Expectancy)':'LifeExpectancy',
                      'Freedom':'FreedomToMakeLifeChoices',
                      'Trust (Government Corruption)':'PerceptionsOfCorruption'
                      }, inplace=True)


df_15['Year'] = 2015


#****** 2016 data ******#
obj = client.get_object(Bucket='edu.gwu.ds.cc.g49487749',Key='2016.csv')
data = obj['Body'].read().decode('utf-8')
df_16 = pd.read_csv(io.StringIO(data), low_memory=False)
df_16.head()


df_16.drop(['Lower Confidence Interval','Upper Confidence Interval','Dystopia Residual'], axis=1, inplace=True)

df_16.rename(columns={'Happiness Rank':'Rank',
                      'Happiness Score':'Score',
                      'Economy (GDP per Capita)':'GDPperCapita',
                      'Family':'SocialSupport',
                      'Health (Life Expectancy)':'LifeExpectancy',
                      'Freedom':'FreedomToMakeLifeChoices',
                      'Trust (Government Corruption)':'PerceptionsOfCorruption'
                      }, inplace=True)

df_16['Year'] = 2016


# Sort the columns
df_16 = df_16[df_15.columns]


# Add back missing regions
df_region = df_16[['Country','Region']]

data = [['Mozambique','Sub-Saharan Africa'],
        ['Lesotho','Sub-Saharan Africa'],
        ['Central African Republic','Sub-Saharan Africa'],
        ['Trinidad & Tobago', 'Latin America and Caribbean'],
        ['Northern Cyprus', 'Middle East and Northern Africa'],
        ['North Macedonia','Central and Eastern Europe'],
        ['Gambia', 'Sub-Saharan Africa'],
        ['Swaziland', 'Sub-Saharan Africa']]

df_add = pd.DataFrame(data, columns=['Country', 'Region'])
df_region = pd.concat([df_region, df_add])


#****** 2017 data ******#
obj = client.get_object(Bucket='edu.gwu.ds.cc.g49487749',Key='2017.csv')
data = obj['Body'].read().decode('utf-8')
df_17 = pd.read_csv(io.StringIO(data), low_memory=False)
df_17.head()


df_17.drop(['Whisker.high','Whisker.low','Dystopia.Residual'], axis=1, inplace=True)

df_17.rename(columns={'Happiness.Rank':'Rank',
                      'Happiness.Score':'Score',
                      'Economy..GDP.per.Capita.':'GDPperCapita',
                      'Family':'SocialSupport',
                      'Health..Life.Expectancy.':'LifeExpectancy',
                      'Freedom':'FreedomToMakeLifeChoices',
                      'Trust..Government.Corruption.':'PerceptionsOfCorruption'
                      }, inplace=True)

df_17['Year'] = 2017


# Use consistent naming for Taiwan and Hong Kong
check = df_17[df_17.Country == 'Taiwan Province of China']
#print(check) # Taiwan = 32

check2 = df_17[df_17.Country == 'Hong Kong S.A.R., China']
#print(check2) # Hong Kong = 70

df_17.iloc[32:33,0:1] = 'Taiwan'
df_17.iloc[70:71,0:1] = 'Hong Kong'

df_17_new2 = pd.merge(df_17, df_region, on='Country', how='left')

df_17 = df_17_new2[df_15.columns]


#****** 2018 data ******#
obj = client.get_object(Bucket='edu.gwu.ds.cc.g49487749',Key='2018.csv')
data = obj['Body'].read().decode('utf-8')
df_18 = pd.read_csv(io.StringIO(data), low_memory=False)
df_18.head()


df_18.rename(columns={'Overall rank':'Rank',
                      'Country or region':'Country',
                      'GDP per capita':'GDPperCapita',
                      'Social support':'SocialSupport',
                      'Healthy life expectancy':'LifeExpectancy',
                      'Freedom to make life choices':'FreedomToMakeLifeChoices',
                      'Perceptions of corruption':'PerceptionsOfCorruption'
                      }, inplace=True)

df_18['Year'] = 2018

df_18_new = pd.merge(df_18, df_region, on='Country', how='left')

df_18 = df_18_new[df_15.columns]


#****** 2019 data ******#
obj = client.get_object(Bucket='edu.gwu.ds.cc.g49487749',Key='2019.csv')
data = obj['Body'].read().decode('utf-8')
df_19 = pd.read_csv(io.StringIO(data), low_memory=False)
df_19.head()


df_19.rename(columns={'Overall rank':'Rank',
                      'Country or region':'Country',
                      'GDP per capita':'GDPperCapita',
                      'Social support':'SocialSupport',
                      'Healthy life expectancy':'LifeExpectancy',
                      'Freedom to make life choices':'FreedomToMakeLifeChoices',
                      'Perceptions of corruption':'PerceptionsOfCorruption'
                      }, inplace=True)

df_19['Year'] = 2019

df_19_new = pd.merge(df_19, df_region, on='Country', how='left')

df_19 = df_19_new[df_15.columns]


# Merge the files
hp = pd.concat([df_15, df_16, df_17, df_18, df_19])

# Check missing values
#print(hp.isnull().sum())

# Imputed with the mean score across years of the same country
hp['PerceptionsOfCorruption'] = hp['PerceptionsOfCorruption'].fillna(hp.groupby('Country')['PerceptionsOfCorruption'].transform('mean'))

#print(hp.isnull().sum())

# Calculate the rank group (top, middle, bottom)
rank_group = hp.groupby('Country').mean().sort_values(by='Score', ascending=False)
top10 = rank_group.iloc[0:10]
top10_country = top10.index

bottom10 = rank_group.iloc[-10:]
bottom10_country = bottom10.index

hp['Top10'] = hp['Country'].apply(lambda x: 2 if x in top10_country else 0)
hp['Bottom10'] = hp['Country'].apply(lambda x: 1 if x in bottom10_country else 0)
hp['RankGroup'] = hp['Top10'] + hp['Bottom10']
hp['RankGroup'].replace({2:'Top', 1:'Bottom', 0:'Middle'}, inplace=True)

# Descriptive statistics
print("Happiness mean = %.2f" %np.mean(hp.Score))
print("Happiness SD = %.2f" %np.std(hp.Score))
print(hp.shape)

pd.set_option('display.max_columns', 15)
data = {'Min':[hp.GDPperCapita.min(), hp.SocialSupport.min(), hp.LifeExpectancy.min(), hp.FreedomToMakeLifeChoices.min(), hp.PerceptionsOfCorruption.min(), hp.Generosity.min()],
        'Max':[hp.GDPperCapita.max(), hp.SocialSupport.max(), hp.LifeExpectancy.max(), hp.FreedomToMakeLifeChoices.max(), hp.PerceptionsOfCorruption.max(), hp.Generosity.max()]}
summary = pd.DataFrame(data, index=['GDPperCapita', 'SocialSupport','LifeExpectancy','FreedomToMakeLifeChoices', 'PerceptionsOfCorruption', 'Generosity'])
print(summary.round(4))

hp.head()

hp.to_csv('happiness_clean_cc.csv')

