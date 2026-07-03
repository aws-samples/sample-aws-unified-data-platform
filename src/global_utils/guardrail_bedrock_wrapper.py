# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Guardrail Bedrock Wrapper - Enforces guardrails on all Bedrock API calls
"""

import json
import os
from typing import Dict, Union

import boto3

from .config_loader import load_configs
from .logger import get_logger

logger = get_logger(__name__)
cfg = load_configs(os.environ.get("ENV"))


class GuardrailBedrockWrapper:
    def __init__(self, region_name: str = None):
        self.region = region_name or cfg.region
        self.bedrock_rt = boto3.client("bedrock-runtime", region_name=self.region)
        self.secrets_client = boto3.client("secretsmanager", region_name=self.region)
        self._guardrail_cache = None

    def _get_guardrail_config(self) -> Dict:
        """Get guardrail config from Secrets Manager"""
        if self._guardrail_cache:
            return self._guardrail_cache

        try:
            env = os.environ.get("ENV", "tf")
            response = self.secrets_client.get_secret_value(
                SecretId=f"guardrail-detail-{env}"
            )
            self._guardrail_cache = json.loads(response["SecretString"])
            return self._guardrail_cache
        except Exception as e:
            logger.warning(f"Failed to get guardrail config: {e}")
            return {}

    def _add_guardrails(self, params: Dict) -> Dict:
        """Add guardrail config if enabled"""
        has_guardrails = hasattr(cfg.mapping.bedrock, "guardrails")
        enabled = (
            has_guardrails and cfg.mapping.bedrock.guardrails.enabled
            if has_guardrails
            else False
        )

        logger.info(f"Guardrail check: has_config={has_guardrails}, enabled={enabled}")

        if has_guardrails and enabled:
            guardrail_config = self._get_guardrail_config()
            if guardrail_config:
                params["guardrailConfig"] = {
                    "guardrailIdentifier": guardrail_config.get("guardrail_id"),
                    "guardrailVersion": guardrail_config.get(
                        "guardrail_version", "DRAFT"
                    ),
                    "trace": "enabled",
                }
                logger.info(
                    f"GUARDRAILS ON - Applied: {guardrail_config.get('guardrail_id')}"
                )
            else:
                logger.warning("GUARDRAILS OFF - Config unavailable")
        else:
            logger.info("GUARDRAILS OFF - Disabled in config")
        return params

    def invoke_model(self, model_id: str, body: Union[str, Dict], **kwargs) -> Dict:
        """Invoke model with guardrails"""
        body_dict = json.loads(body) if isinstance(body, str) else body.copy()
        body_dict = self._add_guardrails(body_dict)

        # Enable trace to see guardrail blocking details
        extra_headers = kwargs.get("ExtraHeaders", {})
        extra_headers["X-Amzn-Bedrock-Trace"] = "ENABLED"
        kwargs["ExtraHeaders"] = extra_headers

        return self.bedrock_rt.invoke_model(
            modelId=model_id, body=json.dumps(body_dict), **kwargs
        )

    def converse(self, model_id: str, messages: list, **kwargs) -> Dict:
        """Converse with guardrails"""
        kwargs = self._add_guardrails(kwargs)

        return self.bedrock_rt.converse(modelId=model_id, messages=messages, **kwargs)


# Global instance and convenience functions
guardrail_bedrock = GuardrailBedrockWrapper()


def guardrail_invoke_model(model_id: str, body: Union[str, Dict], **kwargs) -> Dict:
    logger.info("Bedrock call intercepted")
    return guardrail_bedrock.invoke_model(model_id, body, **kwargs)


def guardrail_converse(model_id: str, messages: list, **kwargs) -> Dict:
    logger.info("Bedrock call intercepted")
    response = guardrail_bedrock.converse(model_id, messages, **kwargs)

    # Log the raw response for debugging purposes
    try:
        response_text = (
            response.get("output", {})
            .get("message", {})
            .get("content", [{}])[0]
            .get("text", "")
        )
        truncated = "..." if len(response_text) > 500 else ""
        logger.info(f"RAW BEDROCK RESPONSE: {response_text[:500]}{truncated}")

        # Check if response was blocked by guardrail
        stop_reason = response.get("stopReason")
        if stop_reason == "guardrail_intervened":
            logger.warning("⚠️ GUARDRAIL BLOCKED RESPONSE")

            # Log guardrail config to see which policies are active
            guardrail_config = guardrail_bedrock._get_guardrail_config()
            logger.warning(f"ACTIVE GUARDRAIL: {guardrail_config.get('guardrail_id')}")

            # Extract and log only blocking metadata (no content)
            trace = response.get("trace", {})
            guardrail_trace = trace.get("guardrail", {})
            input_assessment = guardrail_trace.get("inputAssessment", {})

            for _, assessment in input_assessment.items():
                content_policy = assessment.get("contentPolicy", {})
                filters = content_policy.get("filters", [])

                for filter_info in filters:
                    if filter_info.get("detected"):
                        ftype = filter_info.get("type")
                        conf = filter_info.get("confidence")
                        strength = filter_info.get("filterStrength")
                        action = filter_info.get("action")
                        msg = f"BLOCKED: {ftype} | {conf} | {strength} | {action}"
                        logger.warning(msg)

                # Log processing metrics without content
                metrics = assessment.get("invocationMetrics", {})
                if metrics:
                    latency = metrics.get("guardrailProcessingLatency")
                    cov_data = metrics.get("guardrailCoverage", {})
                    coverage = cov_data.get("textCharacters", {})
                    msg = f"PROC: {latency}ms | {coverage}"
                    logger.warning(msg)

    except Exception as e:
        logger.warning(f"Failed to log response details: {e}")

    return response
