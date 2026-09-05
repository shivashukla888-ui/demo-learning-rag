export type LiveCase = {
  id:string; instrument:string; typology:string; risk:number; confidence:number;
  alerts:number; accounts:string; driver:string; color:string; owner:string;
  status:string; age:string; entityLift:number;
  region?:string; assetClass?:string;
};

export type CopilotResult = {
  status:'GENERATED'|'CACHED'|'GATED'|'ABSTAINED'; caseId:string; reason?:string; model?:string;
  analysis?:{summary:string;counterHypothesis:string;nextBestActions:string[];missingEvidence:string[];
    confidenceNote:string;citedEvidenceRefs:string[]};
  playbookRefs:string[]; tokenControl:Record<string,number|boolean|string>;
  grounded:boolean; decisionPolicy:'HUMAN_REVIEW_REQUIRED';
};

export type ManagementMetrics = {
  cases:number; highPriority:number; closed:number; averageRisk:number; source:string;
};

export type KafkaStatus = {
  enabled:boolean; mode:string; acceptedTopic:string; completedTopic:string;
  deadLetterTopic:string; published:number; consumed:number; publishFailures:number;
  processingFailures:number; lastPublishedAt:string; lastConsumedAt:string; lastError:string;
};

export type UploadResult = {
  status:string; deliveryMode?:string; eventPublished?:boolean; eventId?:string; fingerprint?:string;
};

export type DailyBatchReadiness = {
  batchId:string; businessDate:string; region:string; alertsAvailable:boolean;
  parquetAvailable:boolean; parquetFiles:number; ready:boolean; status:string; checkedAt:string;
};

export type DailyUploadResult = {
  batchId:string; businessDate:string; region:string; dataset:string; filename?:string;
  status:string; fingerprint?:string; fileCount?:number; files?:Array<Record<string,unknown>>;
  readiness:DailyBatchReadiness;
};

const base = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8080';
const key = process.env.NEXT_PUBLIC_AEGIS_API_KEY || 'hackathon-local-change-me';
const headers = {'X-Aegis-Key': key};

async function request<T>(path:string, init:RequestInit={}):Promise<T>{
  const response=await fetch(`${base}${path}`,{...init,headers:{...headers,...init.headers},cache:'no-store'});
  if(!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response.json() as Promise<T>;
}

export const surveillanceApi = {
  cases:()=>request<LiveCase[]>('/v1/cases'),
  management:()=>request<ManagementMetrics>('/v1/management'),
  kafka:()=>request<KafkaStatus>('/v1/integration/kafka'),
  jobs:()=>request<{jobs:Array<Record<string,unknown>>}>('/v1/ml/file-jobs'),
  upload:async(file:File)=>{
    const body=new FormData(); body.append('file',file);
    return request<UploadResult>('/v1/ingestion/files/trades',{method:'POST',body});
  },
  uploadDailyAlerts:async(file:File,region:string,businessDate:string,batchId:string)=>{
    const body=new FormData(); body.append('file',file);
    return request<DailyUploadResult>(`/v1/ingestion/daily-batches/${encodeURIComponent(region)}/${encodeURIComponent(businessDate)}/${encodeURIComponent(batchId)}/alerts`,{method:'POST',body});
  },
  uploadDailyParquet:async(files:File[],region:string,businessDate:string,batchId:string)=>{
    const body=new FormData(); files.forEach(file=>body.append('files',file));
    return request<DailyUploadResult>(`/v1/ingestion/daily-batches/${encodeURIComponent(region)}/${encodeURIComponent(businessDate)}/${encodeURIComponent(batchId)}/trades`,{method:'POST',body});
  },
  dailyReadiness:(region:string,businessDate:string,batchId:string)=>request<DailyBatchReadiness>(
    `/v1/ingestion/daily-batches/${encodeURIComponent(region)}/${encodeURIComponent(businessDate)}/${encodeURIComponent(batchId)}/readiness`),
  copilot:(caseId:string,payload:Record<string,unknown>)=>request<CopilotResult>(
    `/v1/cases/${encodeURIComponent(caseId)}/copilot`,{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)
    }),
  decide:(caseId:string,disposition:string,reason:string)=>request<Record<string,unknown>>(
    `/v1/cases/${encodeURIComponent(caseId)}/decisions`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({investigatorId:'authenticated',role:'INVESTIGATOR',
        disposition:disposition.toUpperCase().replaceAll(' ','_'),reason})
    }),
};
