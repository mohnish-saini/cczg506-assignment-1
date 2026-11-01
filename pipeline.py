from pathlib import Path
import os,json,hashlib,time,platform
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score,precision_score,recall_score,f1_score,average_precision_score,roc_auc_score,confusion_matrix,PrecisionRecallDisplay
import joblib
from prefect import flow,task,get_run_logger
from prefect.artifacts import create_markdown_artifact
ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data'; OUT=ROOT/'artifacts'; OUT.mkdir(exist_ok=True)
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.facecolor':'white'})
def savejson(name,obj):
 temp=OUT/(name+'.tmp');temp.write_text(json.dumps(obj,indent=2,default=str));temp.replace(OUT/name)
def savefig(fig,name,dpi):
 temp=OUT/(name+'.tmp.png');fig.savefig(temp,dpi=dpi);temp.replace(OUT/name)
@task(retries=2,retry_delay_seconds=5)
def ingest():
 p=DATA/'creditcard.csv'
 if not p.exists():
  import urllib.request
  DATA.mkdir(exist_ok=True)
  urllib.request.urlretrieve('https://storage.googleapis.com/download.tensorflow.org/data/creditcard.csv',p)
 d=pd.read_csv(p)
 assert set(d.Class.unique())=={0,1} and len(d)>10000
 assert d.shape[1]==31 and all(pd.api.types.is_numeric_dtype(d[c]) for c in d)
 assert not d.Class.isna().any()
 get_run_logger().info('INGEST rows=%s columns=%s missing=%s',len(d),d.shape[1],int(d.isna().sum().sum()))
 return d
@task
def prepare(d):
 raw=len(d); duplicates=int(d.duplicated().sum());d=d.drop_duplicates().reset_index(drop=True)
 X=d.drop(columns='Class');y=d.Class
 a,b,ya,yb=train_test_split(X,y,test_size=.30,stratify=y,random_state=42)
 a=a.copy();b=b.copy();rng=np.random.default_rng(42);mask={}
 for label,frame in [('train',a),('test',b)]:
  for c in ['Amount','V1','V2']:
   idx=rng.choice(frame.index,size=int(len(frame)*.01),replace=False);frame.loc[idx,c]=np.nan
   mask[label+'_'+c]=len(idx)
 meta={'raw_rows':raw,'raw_fraud':int(pd.read_csv(DATA/'creditcard.csv',usecols=['Class']).Class.sum()),'duplicates_removed':duplicates,'rows':len(d),'fraud':int(y.sum()),'columns':31,'train_rows':len(a),'test_rows':len(b),'train_fraud':int(ya.sum()),'test_fraud':int(yb.sum()),'original_missing':0,'induced_missing':mask,'dataset_sha256':hashlib.sha256((DATA/'creditcard.csv').read_bytes()).hexdigest(),'seed':42,'dtypes':d.dtypes.astype(str).to_dict(),'environment':{'python':platform.python_version(),'sklearn':__import__('sklearn').__version__,'prefect':__import__('prefect').__version__}}
 savejson('data.json',meta);a.describe().round(4).to_csv(OUT/'summary.csv');a.dtypes.astype(str).to_csv(OUT/'dtypes.csv')
 get_run_logger().info('PREPROCESS duplicates=%s split=%s/%s induced_NaNs=%s; label unchanged',duplicates,len(a),len(b),sum(mask.values()))
 return a,b,ya,yb
@task
def explore(parts):
 a,b,ya,yb=parts
 med=SimpleImputer(strategy='median');clean=pd.DataFrame(med.fit_transform(a),columns=a.columns,index=a.index)
 scaled=pd.DataFrame(StandardScaler().fit_transform(clean),columns=clean.columns);scaled.describe().to_csv(OUT/'normalized_summary.csv')
 bands=pd.cut(clean.Amount,bins=[-np.inf,10,100,500,np.inf],labels=['0 to 10','10 to 100','100 to 500','over 500'],right=True)
 encoded=pd.get_dummies(bands,prefix='amount_band',dtype=int);encoded.head(10).to_csv(OUT/'encoding.csv',index=False)
 rates=pd.DataFrame({'band':bands,'fraud':ya}).groupby('band',observed=False).fraud.agg(['count','sum','mean']);rates.to_csv(OUT/'bands.csv')
 corr=clean.assign(Class=ya).corr();corr.to_csv(OUT/'correlations.csv')
 fig,ax=plt.subplots(1,2,figsize=(10,3.4));counts=ya.value_counts().sort_index();ax[0].bar(['Legitimate','Fraud'],counts,color=['#154b66','#d97732']);ax[0].set_yscale('log');ax[0].set_title('Training class distribution log scale');ax[0].set_ylabel('Transactions');ax[1].hist(np.log1p(clean.Amount),bins=45,color='#154b66');ax[1].set_title('Training transaction amount');ax[1].set_xlabel('log 1 plus Amount');fig.tight_layout();savefig(fig,'univariate.png',180);plt.close(fig)
 fig,ax=plt.subplots(1,2,figsize=(10,3.4));ax[0].bar(rates.index.astype(str),rates['mean']*100,color='#154b66');ax[0].set_ylabel('Fraud rate percent');ax[0].tick_params(axis='x',labelsize=8);ax[0].set_title('Amount band and fraud rate');v=corr.Class.drop('Class').sort_values().head(7);ax[1].barh(v.index,v.values,color='#d97732');ax[1].set_title('Strong negative label correlations');ax[1].set_xlabel('Pearson r');fig.tight_layout();savefig(fig,'bivariate.png',180);plt.close(fig)
 sel=['Amount','V1','V2','V4','V10','V12','V14','V17','Class'];fig,ax=plt.subplots(figsize=(7,5));im=ax.imshow(corr.loc[sel,sel],vmin=-1,vmax=1,cmap='RdBu_r');ax.set_xticks(range(9),sel,rotation=45);ax.set_yticks(range(9),sel);fig.colorbar(im,ax=ax,label='Pearson r');fig.tight_layout();savefig(fig,'heatmap.png',160);plt.close(fig)
 report={'run_time_utc':pd.Timestamp.now(tz='UTC').isoformat(),'train_amount_mean':float(a.Amount.mean()),'missing_before_imputation':int(a.isna().sum().sum()),'missing_after_imputation':int(clean.isna().sum().sum()),'normalized_amount_mean':float(scaled.Amount.mean()),'normalized_amount_std':float(scaled.Amount.std(ddof=0)),'correlations_with_label':corr.Class.drop('Class').sort_values().to_dict(),'band_rates':rates.reset_index().to_dict('records'),'median_amount':float(clean.Amount.median())}
 savejson('eda.json',report)
 get_run_logger().info('EDA correlations, binning, one-hot encoding and 3 chart files saved; NaNs after imputation=0')
 return report
@flow(name='fraud-data-pipeline',log_prints=True)
def data_pipeline():
 d=ingest();parts=prepare(d);report=explore(parts)
 create_markdown_artifact(key='data-quality',markdown=f"# Data quality\nRows: {len(d)}\n\nMissing after imputation: {report['missing_after_imputation']}\n\nAmount mean after standardization: {report['normalized_amount_mean']:.6f}")
 # Re-log frozen holdout reference metrics; this is not production label monitoring.
 if (OUT/'metrics.json').exists():
  m=json.loads((OUT/'metrics.json').read_text());get_run_logger().info('REFERENCE MODEL METRICS %s',json.dumps(m['models']))
 return report
@task
def train_models(parts):
 a,b,ya,yb=parts;models={};preds={};fitted={}
 algs={'LogisticRegression':LogisticRegression(class_weight='balanced',max_iter=1000,random_state=42),'RandomForest':RandomForestClassifier(n_estimators=80,max_depth=14,min_samples_leaf=2,class_weight='balanced_subsample',n_jobs=2,random_state=42)}
 for name,clf in algs.items():
  p=Pipeline([('imputer',SimpleImputer(strategy='median')),('scaler',StandardScaler()),('model',clf)])
  t=time.perf_counter();p.fit(a,ya);secs=time.perf_counter()-t;s=p.predict_proba(b)[:,1];hat=(s>=.5).astype(int)
  m={'accuracy':accuracy_score(yb,hat),'precision':precision_score(yb,hat,zero_division=0),'recall':recall_score(yb,hat),'f1':f1_score(yb,hat),'average_precision':average_precision_score(yb,s),'roc_auc':roc_auc_score(yb,s),'fit_seconds':secs,'threshold':.5,'confusion_matrix':confusion_matrix(yb,hat).tolist()};models[name]=m;preds[name]=s;fitted[name]=p
  joblib.dump(p,OUT/(name+'.joblib'));get_run_logger().info('MODEL %s %s',name,json.dumps(m))
 # Deployment candidate is declared in advance for this demonstration, not selected by optimizing test scores.
 chosen='RandomForest';joblib.dump(fitted[chosen],OUT/'model.joblib')
 savejson('metrics.json',{'evaluated_utc':pd.Timestamp.now(tz='UTC').isoformat(),'models':models,'deployment_candidate':chosen,'selection_policy':'RandomForest declared in advance; no test-based threshold optimization','features':list(a.columns)})
 imp=pd.Series(fitted['RandomForest'].named_steps['model'].feature_importances_,index=a.columns).sort_values(ascending=False);imp.to_csv(OUT/'importance.csv')
 fig,ax=plt.subplots(1,2,figsize=(10,3.6));imp.head(10).sort_values().plot.barh(ax=ax[0],color='#154b66');ax[0].set_title('Random forest training importance');ax[0].set_xlabel('Mean impurity decrease');
 for name,s in preds.items(): PrecisionRecallDisplay.from_predictions(yb,s,name=name,ax=ax[1])
 ax[1].set_title('Holdout precision recall curves');fig.tight_layout();savefig(fig,'models.png',180);plt.close(fig)
 create_markdown_artifact(key='model-evaluation',markdown=pd.DataFrame(models).T.drop(columns='confusion_matrix').to_markdown())
 # Known examples for API demonstration, not a separate validation set.
 sample=b.loc[yb[yb==1].index[0]].fillna(a.median()).to_dict();savejson('sample_request.json',{'features':sample})
 return models
@flow(name='fraud-training-pipeline',log_prints=True)
def training_pipeline():
 return train_models(prepare(ingest()))
if __name__=='__main__':
 import sys
 if '--serve' in sys.argv:
  data_pipeline.serve(name='fraud-data-every-2-minutes',interval=120,limit=1)
 else:
  data_pipeline();training_pipeline()
