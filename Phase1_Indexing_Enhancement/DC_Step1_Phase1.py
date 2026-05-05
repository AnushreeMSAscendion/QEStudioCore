import os
import json
import time
import requests
import concurrent.futures
from typing import Dict, Any, List, Union, Optional
import AVASecret
# Assuming these are available in your environment based on previous context
from app.helpers.redis_logs import PipelineAILogs
try:
    from app.core.redis_client import redis_client
except ImportError:
    redis_client = None

# Mocking the input class structure based on your snippet
class TestSuiteOrchestratorInput:
    def __init__(self, username: str = "system_user"):
        self.aava_username = username

class DynamicContextualizer:
    def __init__(self, aava_api_token: str, pipeline_url: str):
        # Base AAVA API Config
        self.token = AVASecret.getValue("access_key_hp_user")
        self.PIPELINE_URL = "http://api-workflows.ascendion.svc.cluster.local/workflows/workflow-executions"
        self.RESULT_URL_TEMPLATE = f"{self.PIPELINE_URL}/{{}}/result"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Pipeline Engine Config
        self.PIPELINE_URL = pipeline_url
        self.RESULT_URL_TEMPLATE = f"{self.PIPELINE_URL.rstrip('/')}/{{}}/result"
        self.redis_client = redis_client

    # ==========================================
    # AAVA PIPELINE EXECUTION ENGINE
    # ==========================================
    def _trigger_workflow(self, pipeline_id: str, inputs: TestSuiteOrchestratorInput, placeholders: Dict[str, Any]) -> Optional[str]:
        PipelineAILogs().publishLogs(f"AAVA Orchestration Pipeline Triggering WF#{pipeline_id} at {self.PIPELINE_URL}", "blue", redisClient=self.redis_client)
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {
            "pipelineId": pipeline_id,
            "user": inputs.aava_username,
            "userInputs": json.dumps(placeholders),
            "priority": "1"
        }
        form_data = {k: (None, str(v)) for k, v in payload.items()}
        try:
            response = requests.post(self.PIPELINE_URL, headers=headers, files=form_data, timeout=120, verify=False)
            response.raise_for_status()
            data = response.json()
            return data.get("workflowExecutionId") or data.get("data", {}).get("workflowExecutionId")
        except Exception as e:
            # === THE MAGIC TRICK ===
            safe_headers = {"Authorization": f"Bearer {str(self.token)[:15]}..."}
            PipelineAILogs().publishLogs(f"AAVA Orchestration ERROR {e}", "red", redisClient=self.redis_client)
            
            debug_msg = (
                f"CRASH in WF#{pipeline_id}. "
                f"URL: {self.PIPELINE_URL} | "
                f"Error: {str(e)} | "
                f"Headers: {safe_headers} | "
                f"Payload: {json.dumps(payload)}"
            )
            # Raise custom massive error for UI interception
            raise Exception(debug_msg)

    def _poll_execution(self, execution_id: str) -> Dict[str, Any]:
        url = self.RESULT_URL_TEMPLATE.format(execution_id)
        headers = {"Authorization": f"Bearer {self.token}"}
        while True:
            try:
                response = requests.get(url, headers=headers, timeout=60, verify=False)
                response.raise_for_status()
                result = response.json()
                
                status = result.get("status")
                inner_data = result.get("data")
                inner_status = inner_data.get("status") if isinstance(inner_data, dict) else None
                
                if status in ["SUCCESS", "FAILED", "ERROR"] and inner_status not in ["QUEUED", "IN_PROGRESS"]:
                    if status == "SUCCESS":
                        PipelineAILogs().publishLogs(f"WF {execution_id} finished: {status}", "blue", redisClient=self.redis_client)
                        print(f"WF {execution_id} finished: {status}")
                    else:
                        PipelineAILogs().publishLogs(f"WF {execution_id} finished with error status: {status}", "red", redisClient=self.redis_client)
                        print(f"WF {execution_id} finished with error status: {status}")
                        raise Exception("WF failed.")
                    return result
                    
                time.sleep(20)
            except Exception as e:
                PipelineAILogs().publishLogs(f"Polling error {execution_id}: {e}", "orange", redisClient=self.redis_client)
                print(f"Polling error {execution_id}: {e}")
                time.sleep(20)

    def _parse_nested_response(self, result_data: Dict[str, Any]) -> str:
        try:
            res_content = result_data.get("result") or result_data.get("data", {}).get("result") or result_data.get("data")
            if isinstance(res_content, str):
                try: res_content = json.loads(res_content)
                except: pass
                
            response_payload = res_content.get("response", "{}") if isinstance(res_content, dict) else res_content
            if isinstance(response_payload, str):
                try:
                    p = json.loads(response_payload)
                    return p.get("output", json.dumps(p))
                except: return response_payload
            return json.dumps(response_payload)
        except Exception as e:
            PipelineAILogs().publishLogs(f"Error parsing AAVA response: {e}", "red", redisClient=self.redis_client)
            return "{}"

    def _execute_and_fetch(self, wf_id: str, inputs: TestSuiteOrchestratorInput, placeholders: Dict[str, Any], step_name: str) -> Dict[str, Any]:
        exec_id = self._trigger_workflow(wf_id, inputs, placeholders)
        if not exec_id:
            raise Exception(f"Failed to trigger {step_name} workflow.")
        result = self._poll_execution(exec_id)
        parsed_output = self._parse_nested_response(result)
        return {
            "step": step_name,
            "response": parsed_output,
            "raw_result": result
        }

    # ==========================================
    # WORKFLOW TRIGGERS (NOW USING ACTUAL EXECUTION ENGINE)
    # ==========================================
    def _call_github_workflow(self, github_path: str, inputs: TestSuiteOrchestratorInput) -> List[Dict[str, str]]:
        """Trigger GitHub workflow to fetch files using the robust execution engine."""
        print(f"[Workflow] Triggering GitHub Fetch Workflow for path: {github_path}...")
        
        placeholders = {"target_path": github_path}
        # Replace 'xxxx' with your actual GitHub fetch workflow ID
        #### change here
        result = self._execute_and_fetch("xxxx", inputs, placeholders, "GitHub Fetch")
        
        try:
            # We expect the pipeline to return a JSON string that parses into a List of Dicts
            # containing "file name" and "content" keys.
            parsed_files = json.loads(result["response"])
            if isinstance(parsed_files, list):
                return parsed_files
            else:
                print("Warning: GitHub workflow response was not a list. Using empty fallback.")
                return []
        except json.JSONDecodeError as e:
            print(f"Failed to decode GitHub workflow output: {e}. Raw response: {result['response']}")
            return []


    # ==========================================
    # API METHODS (Original Working Baseline)
    # ==========================================
    def _get_agent(self, agent_id: Union[int, str]) -> Dict[str, Any]:
        url = f"{self.base_url}/agents?agentId={agent_id}"
        resp = requests.get(url, headers=self.headers, verify=False, timeout=30)
        
        if resp.status_code != 200:
            raise ValueError(f"GET Agent Failed ({resp.status_code}): {resp.text}")
            
        data = resp.json()
        if data.get("status") != "SUCCESS":
            raise ValueError(f"API Error during GET: {data}")
            
        return data["data"]["agentDetail"]

    def _update_agent(self, agent_detail: Dict[str, Any], new_kb_ids: List[int], file_data: Dict[str, str]) -> Union[int, str]:
        url = f"{self.base_url}/agents"
        final_kb_ids = new_kb_ids  

        agent_configs = agent_detail.get("agentConfigs", {})
        existing_tools = [
            t.get("toolId") for t in agent_configs.get("userToolRef", []) 
            if t.get("toolId") is not None
        ]
        
        if "tools" in agent_detail and isinstance(agent_detail["tools"], list):
            existing_tools.extend(agent_detail["tools"])
            
        final_tool_ids = list(set(existing_tools))
        
        model_id = agent_detail.get("modelId")
        if not model_id:
            model_refs = agent_detail.get("agentConfigs", {}).get("modelRef", [])
            if model_refs:
                model_id = model_refs[0].get("modelId")

        target_realm_id = agent_detail.get("realmId")
        extracted_team_id = None
        put_call_team_id = agent_detail.get("teamId") 

        if target_realm_id is not None:
            realms_api = f"{self.base_url}/api/auth/realms"
            resp_realms = requests.get(realms_api, headers=self.headers, verify=False, timeout=30)
            if resp_realms.status_code == 200:
                realms_data = resp_realms.json()
                if realms_data.get("status") == "SUCCESS":
                    realm_list = realms_data.get("data", {}).get("realmList", [])
                    for realm in realm_list:
                        if realm.get("realmId") == target_realm_id:
                            extracted_team_id = realm.get("teamId")
                            break

        if extracted_team_id is not None:
            put_call_team_id = extracted_team_id

        # Mapping content securely into the payload. Using file_data to inject code context
        # Making sure 'file name' acts as the specific automation script's path reference.
        script_path_context = f"Context file: {file_data.get('file name', 'unknown_script_path.py')}\n\n"
        
        payload = {
            "agentConfigs": agent_detail.get("agentConfigs", {}),
            "agentDetails": script_path_context + file_data.get("content", agent_detail.get("agentDetails", "")),
            "backstory": agent_detail.get("backstory", ""),
            "description": agent_detail.get("description", ""),
            "expectedOutput": agent_detail.get("expectedOutput", ""),
            "goal": agent_detail.get("goal", ""),
            "id": agent_detail.get("id"),
            "inputFields": agent_detail.get("inputFields"),
            "kbIds": final_kb_ids,           
            "modelId": model_id,
            "name": agent_detail.get("name", ""),
            "practiceArea": agent_detail.get("practiceArea"),
            "role": agent_detail.get("role", ""),
            "status": "CREATED",             
            "tags": agent_detail.get("tags", []),
            "teamId": put_call_team_id,                      
            "tools": final_tool_ids          
        }

        resp = requests.put(url, headers=self.headers, json=payload, verify=False, timeout=30)
        if resp.status_code != 200:
            raise ValueError(f"PUT Update Agent Failed ({resp.status_code}): {resp.text}")

        resp_data = resp.json()
        if resp_data.get("status") != "SUCCESS":
            raise ValueError(f"API Error during UPDATE: {resp_data}")

        return resp_data.get("data", {}).get("agentId", agent_detail["id"])

    def _send_to_review(self, agent_id: Union[int, str]):
        url = f"{self.base_url}/agents/IN_REVIEW?agent-id={agent_id}"
        resp = requests.put(url, headers=self.headers, json={}, verify=False, timeout=30)
        if resp.status_code != 200 or resp.json().get("status") != "SUCCESS":
            raise ValueError(f"PUT In-Review Failed: {resp.text}")

    def _approve_agent(self, agent_id: Union[int, str]):
        url = f"{self.base_url}/agents/approval"
        payload = {
            "comments": {"whatWentGood": "good", "whatWentWrong": "", "improvements": ""},
            "id": agent_id,
            "status": "APPROVED"
        }
        resp = requests.put(url, headers=self.headers, json=payload, verify=False, timeout=30)
        if resp.status_code != 200 or resp.json().get("status") != "SUCCESS":
            raise ValueError(f"PUT Approval Failed: {resp.text}")

    # ==========================================
    # MAIN ORCHESTRATION LOGIC
    # ==========================================
    def execute_github_batch_contextualization(self, github_path: str, new_kb_ids: Union[int, List[int]], inputs: TestSuiteOrchestratorInput):
        if isinstance(new_kb_ids, int):
            new_kb_ids = [new_kb_ids]
            
        results = []
            
        try:
            # 1. Trigger GitHub workflow to get files via execution engine
            retrieved_files = self._call_github_workflow(github_path, inputs)
            
            if not retrieved_files:
                print("No files retrieved from GitHub workflow. Exiting.")
                return {"status": "SUCCESS", "processed_files": 0}

            # 2. Iterate over each file and run the contextualization process
            for idx, file_data in enumerate(retrieved_files):
                file_name = file_data.get("file name", f"unknown_{idx}")
                print(f"\n--- Processing File: {file_name} ---")
                
                ### change here 
                initial_agent_id = "yyyy"
                
                try:
                    print(f"Fetching configuration for Agent ID: {initial_agent_id}")
                    agent_detail = self._get_agent(initial_agent_id)
                    
                    print(f"Updating Agent with new KB IDs and content from {file_name}")
                    updated_agent_id = self._update_agent(agent_detail, new_kb_ids, file_data)
                    print(f"Agent update response received. Current active ID: {updated_agent_id}")
                    
                    self._send_to_review(updated_agent_id)
                    self._approve_agent(updated_agent_id)
                    
                    results.append({
                        "file_name": file_name,
                        "status": "SUCCESS",
                        "final_agent_id": updated_agent_id
                    })

                except Exception as file_err:
                    print(f"Error processing file {file_name}: {str(file_err)}")
                    results.append({
                        "file_name": file_name,
                        "status": "ERROR",
                        "error": str(file_err)
                    })

            return {
                "status": "BATCH_COMPLETE",
                "total_processed": len(retrieved_files),
                "details": results
            }

        except Exception as e:
            print(f"GitHub Batch Contextualization Error: {str(e)}")
            return {"status": "FATAL_ERROR", "error": str(e)}