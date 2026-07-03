{
  "Comment": "ETL Pipeline with Updated Audit Structure and Failure Handling",
  "StartAt": "ProcessTables",
  "States": {
    "ProcessTables": {
      "Type": "Map",
      "ItemsPath": "$.tables_list",
      "MaxConcurrency": 10,
      "ToleratedFailurePercentage": 100,
      "ResultPath": "$.results",
      "Iterator": {
        "StartAt": "get_table_name",
        "States": {
          "get_table_name": {
            "Type": "Pass",
            "Next": "InitializeAudit",
            "Parameters": {
              "table_name.$": "$"
            }
          },
          "InitializeAudit": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:putItem",
            "Parameters": {
              "TableName": "${audit_table_name}",
              "Item": {
                "stepfunction_execution_id": {
                  "S.$": "$$.Execution.Name"
                },
                "table_name": {
                  "S.$": "$.table_name"
                },
                "stepfunction_name": {
                  "S.$": "$$.StateMachine.Name"
                },
                "stepfunction_exec_start": {
                  "S.$": "$$.State.EnteredTime"
                },
                "stepfunction_exec_status": {
                  "S": "Running"
                },
                "stages": {
                  "M": {}
                }
              }
            },
            "ResultPath": null,
            "Next": "CheckConfigFile"
          },
          "CheckConfigFile": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:headObject",
            "Parameters": {
              "Bucket": "${config_bucket_name}",
              "Key.$": "States.Format('udm/pipeline/{}.yaml', $.table_name)"
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "UpdateConfigFileNotFoundDB",
                "ResultPath": "$.error"
              }
            ],
            "ResultPath": null,
            "Next": "StageJob"
          },
          "UpdateConfigFileNotFoundDB": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "${audit_table_name}",
              "Key": {
                "stepfunction_execution_id": {
                  "S.$": "$$.Execution.Name"
                },
                "table_name": {
                  "S.$": "$.table_name"
                }
              },
              "UpdateExpression": "SET stepfunction_exec_end = :end_time, stepfunction_exec_status = :status, stepfunction_failure_reason = :failure_reason",
              "ExpressionAttributeValues": {
                ":end_time": {
                  "S.$": "$$.State.EnteredTime"
                },
                ":status": {
                  "S": "Failed"
                },
                ":failure_reason": {
                  "S.$": "States.Format('Config file not available: s3://${config_bucket_name}/udm/pipeline/{}.yaml', $.table_name)"
                }
              }
            },
            "Next": "NotifyConfigFileNotFound",
            "ResultPath": null
          },
          "NotifyConfigFileNotFound": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
              "TopicArn": "${udm_pipeline_sns_topic_arn}",
              "Message.$": "States.Format('Config file not available: s3://${config_bucket_name}/udm/pipeline/{}.yaml', $.table_name)",
              "Subject": "[${env_name}] - [Failed] UDM Pipeline Config File Not Found"
            },
            "Next": "ConfigFileNotFound"
          },
          "ConfigFileNotFound": {
            "Type": "Pass",
            "Result": {
              "status": "failed",
              "stage": "config_check"
            },
            "End": true
          },
          "StageJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "${stage_job_name}",
              "Arguments": {
                "--JOB_NAME": "${stage_job_name}",
                "--config_file_path.$": "States.Format('s3://${config_bucket_name}/udm/pipeline/{}.yaml', $.table_name)",
                "--audit_table_name": "${audit_table_name}",
                "--stepfunction_execution_id.$": "$$.Execution.Name",
                "--job_type": "stage",
                "--udm_processed_date.$": "$$.Execution.StartTime",
                "--table_location": "s3://${stage_bucket_name}"
              }
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "catch_stage_error_message",
                "ResultPath": "$.error"
              }
            ],
            "ResultPath": null,
            "Next": "CuratedJob"
          },
          "catch_stage_error_message": {
            "Type": "Pass",
            "Parameters": {
              "error.$": "States.StringToJson($.error.Cause)",
              "table_name.$": "$.table_name"
            },
            "Next": "UpdateStageFailureDB"
          },
          "CuratedJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "${curated_job_name}",
              "Arguments": {
                "--JOB_NAME.$": "States.Format('${curated_job_name}_{}', $.table_name)",
                "--config_file_path.$": "States.Format('s3://${config_bucket_name}/udm/pipeline/{}.yaml', $.table_name)",
                "--audit_table_name": "${audit_table_name}",
                "--stepfunction_execution_id.$": "$$.Execution.Name",
                "--job_type": "curation",
                "--udm_processed_date.$": "$$.Execution.StartTime",
                "--table_location": "s3://${curated_bucket_name}"
              }
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "catch_curated_error_message",
                "ResultPath": "$.error"
              }
            ],
            "ResultPath": null,
            "Next": "UpdateTableAudit"
          },
          "catch_curated_error_message": {
            "Type": "Pass",
            "Parameters": {
              "error.$": "States.StringToJson($.error.Cause)",
              "table_name.$": "$.table_name"
            },
            "Next": "UpdateCuratedFailureDB"
          },
          "UpdateTableAudit": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "${audit_table_name}",
              "Key": {
                "stepfunction_execution_id": {
                  "S.$": "$$.Execution.Name"
                },
                "table_name": {
                  "S.$": "$.table_name"
                }
              },
              "UpdateExpression": "SET stepfunction_exec_end = :end_time, stepfunction_exec_status = :status",
              "ExpressionAttributeValues": {
                ":end_time": {
                  "S.$": "$$.State.EnteredTime"
                },
                ":status": {
                  "S": "Completed"
                }
              }
            },
            "Next": "Success"
          },
          "UpdateStageFailureDB": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "${audit_table_name}",
              "Key": {
                "stepfunction_execution_id": {
                  "S.$": "$$.Execution.Name"
                },
                "table_name": {
                  "S.$": "$.table_name"
                }
              },
              "UpdateExpression": "SET stepfunction_exec_end = :end_time, stepfunction_exec_status = :status, stepfunction_failure_reason = :failure_reason",
              "ExpressionAttributeValues": {
                ":end_time": {
                  "S.$": "$$.State.EnteredTime"
                },
                ":status": {
                  "S": "Failed"
                },
                ":failure_reason": {
                  "S.$": "States.Format('Stage Job failed with error message: {}', $.error.ErrorMessage)"
                }
              }
            },
            "Next": "NotifyStageFailure",
            "ResultPath": null
          },
          "NotifyStageFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
              "TopicArn": "${udm_pipeline_sns_topic_arn}",
              "Message.$": "States.Format('Stage Job failed for table: {}. Error: {}', $.table_name, $.error.ErrorMessage)",
              "Subject": "[${env_name}] - [Failed] UDM Pipeline Stage Job has Failed"
            },
            "Next": "Stage Failed"
          },
          "Stage Failed": {
            "Type": "Pass",
            "Result": {
              "status": "failed",
              "stage": "stage"
            },
            "End": true
          },
          "UpdateCuratedFailureDB": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:updateItem",
            "Parameters": {
              "TableName": "${audit_table_name}",
              "Key": {
                "stepfunction_execution_id": {
                  "S.$": "$$.Execution.Name"
                },
                "table_name": {
                  "S.$": "$.table_name"
                }
              },
              "UpdateExpression": "SET stepfunction_exec_end = :end_time, stepfunction_exec_status = :status, stepfunction_failure_reason = :failure_reason",
              "ExpressionAttributeValues": {
                ":end_time": {
                  "S.$": "$$.State.EnteredTime"
                },
                ":status": {
                  "S": "Failed"
                },
                ":failure_reason": {
                  "S.$": "States.Format('Curated Job failed with error message: {}', $.error.ErrorMessage)"
                }
              }
            },
            "Next": "NotifyCuratedFailure",
            "ResultPath": null
          },
          "NotifyCuratedFailure": {
            "Type": "Task",
            "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
              "TopicArn": "${udm_pipeline_sns_topic_arn}",
              "Message.$": "States.Format('Curated Job failed for table: {}. Error: {}', $.table_name, $.error.ErrorMessage)",
              "Subject": "[${env_name}] - [Failed] UDM Pipeline Curated Job has Failed"
            },
            "Next": "Curated Failed"
          },
          "Curated Failed": {
            "Type": "Pass",
            "Result": {
              "status": "failed",
              "stage": "curated"
            },
            "End": true
          },
          "Success": {
            "Type": "Pass",
            "Result": {
              "status": "success"
            },
            "End": true
          }
        }
      },
      "Next": "CheckForFailures"
    },
    "CheckForFailures": {
      "Type": "Pass",
      "Parameters": {
        "hasFailures.$": "States.ArrayContains($.results[*].status, 'failed')",
        "tables_list.$": "$.tables_list"
      },
      "Next": "EvaluateStatus"
    },
    "EvaluateStatus": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.hasFailures",
          "BooleanEquals": true,
          "Next": "ExecutionFailed"
        }
      ],
      "Default": "FormatSuccessMessage"
    },
    "ExecutionFailed": {
      "Type": "Fail",
      "Cause": "One or more table processing jobs failed"
    },
    "FormatSuccessMessage": {
      "Type": "Pass",
      "Parameters": {
        "message.$": "States.Format('UDM Pipeline completed successfully.\n\nProcessed Tables: {}\nudm_incremental_column: {}\nudm_incremental_value: {}\n\nNote: The next UDM refresh is scheduled to run after four hours.', States.JsonToString($.tables_list), 'udm_processed_date', $$.Execution.StartTime)"
      },
      "Next": "ExecutionSucceeded"
    },
    "ExecutionSucceeded": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${downstream_sns_topic_arn}",
        "Message.$": "$.message",
        "Subject": "[${env_name}] - [Success] UDM Pipeline Execution Completed Successfully"
      },
      "End": true
    }
  }
}