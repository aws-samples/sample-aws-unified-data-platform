# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import os
from types import SimpleNamespace
from typing import Any, Dict

import boto3
import botocore

from .config_loader import load_configs
from .logger import get_logger

logger = get_logger(__name__)


cfg = load_configs(os.environ.get("ENV"))

session = boto3.session.Session(region_name=cfg.region)
region = session.region_name

bedrock_rt = boto3.client(
    "bedrock-runtime",
    region_name=region,
    config=botocore.config.Config(cfg.mapping.bedrock.boto_config),
)
bedrock_agent = boto3.client("bedrock-agent", region_name=cfg.region)


def generate_answer(
    prompt: str,
    system_prompt_cfg=cfg.mapping.prompts.system_default,
    model_id=cfg.mapping.bedrock.model.llm,
    bedrock_cfg=cfg.mapping.bedrock.claude_config,
) -> str:
    """
    Invokes a large language model (LLM) and returns the generated answer.

    Args:
        prompt (str): The input prompt or question to be provided to the LLM.
        system_prompt (namespace): Parameters to retrieve system prompt for
        instructions to be provided to the LLM.
        model_id (str, optional): The ID of the LLM model to be used. Defaults
          to the value in `cfg.mapping.bedrock.model.llm`.
        bedrock_cfg (dict, optional): The configuration parameters for the
          Bedrock runtime. Defaults to the value in `cfg.mapping.bedrock.claude_config`.

    Returns:
        str: The generated answer from the LLM.
    """
    logger.info("Calling bedrock ...")
    message = {
        "messages": [
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "The answer is"},
        ]
    }
    if system_prompt_cfg:
        system_prompt, _ = get_prompt_by_name(
            system_prompt_cfg.bedrock_name, system_prompt_cfg.version
        )
    else:
        system_prompt = ""
    body = {**message, **dict(vars(bedrock_cfg)), "system": system_prompt}
    response = bedrock_rt.invoke_model(modelId=model_id, body=json.dumps(body))
    response = json.loads(response["body"].read().decode("utf-8"))
    return response["content"][0]["text"]


def generate_answer_safe(
    prompt: str,
    system_prompt_cfg: SimpleNamespace,
    model_id=cfg.mapping.bedrock.model.llm,
    bedrock_cfg=cfg.mapping.bedrock.claude_config,
) -> str:
    """
    Invokes a large language model (LLM) and returns the generated answer.
    Gracefully handle errors.

    Args:
        prompt (str): The input prompt or question to be provided to the LLM.
        system_prompt (str): The system prompt or instructions to be provided to the
          LLM.
        model_id (str, optional): The ID of the LLM model to be used. Defaults to the
          value in
        `cfg.mapping.bedrock.model.llm`.
        bedrock_cfg (dict, optional): The configuration parameters for the
        Bedrock runtime.
        Defaults to the value in `cfg.mapping.bedrock.claude_config`.

    Returns:
        str: The generated answer from the LLM.
    """
    try:
        return generate_answer(prompt, system_prompt_cfg, model_id, bedrock_cfg)
    except bedrock_rt.exceptions.ThrottlingException as e:
        logger.error(f"throttled {e}")
        return None
    except bedrock_rt.exceptions.ServiceUnavailableException as e:
        logger.error(f"throttled {e}")
        return None


def get_prompt(prompt_cfg: SimpleNamespace) -> tuple:
    """
    Retrieves a prompt from the Amazon Bedrock Prompt Management Service.

    Usage:

        prompt, prompt_vars = get_prompt(
                cfg.mapping.prompts.loc_harmonizer.id,
                cfg.mapping.prompts.loc_harmonizer.version)

    Args:
        prompt_cfg (SimpleNamespace): Config object with two attributes id and version

    Returns:
        template (str), vars (list of key-values) : Prompt text, and a dictionary
        containing the prompt details, including the name, description, and content.
    """
    logger.info(f"Getting prompt {prompt_cfg}")
    prompt_version = getattr(prompt_cfg, "version", None)
    prompt_id = prompt_cfg.id
    try:
        if prompt_version:
            prompt = bedrock_agent.get_prompt(
                promptIdentifier=prompt_id, promptVersion=prompt_version
            )
        else:
            prompt = bedrock_agent.get_prompt(promptIdentifier=prompt_id)
    except Exception as e:
        logger.error(
            f"Error: Could not retrieve prompt {prompt_id}-{prompt_version} \n {e}"
        )
        # TODO: Use dynamo-db instead of bedrock
        raise e

    prompt_vars = [
        list(i.values())[0]
        for i in prompt["variants"][0]["templateConfiguration"]["text"][
            "inputVariables"
        ]
    ]
    template = prompt["variants"][0]["templateConfiguration"]["text"]["text"]

    return template, prompt_vars


def fill_prompt_template(template: str, variables: Dict[str, Any]) -> str:
    """
    Replaces {{var}} placeholders in the template with corresponding values
    from the variables dict.

    Parameters:
        template (str): Template containing {{var}} placeholders.
        variables (dict): Dictionary of replacements,
        where key is variable name (no curly braces).

    Returns:
        str: Filled-in template with all placeholders replaced.
    """
    result = template
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"  # e.g., '{{example}}'
        result = result.replace(placeholder, value)
    return result


def get_prompt_by_name(prompt_name: str, prompt_version: str = None) -> tuple:
    """
    Retrieves a prompt from the Amazon Bedrock Prompt Management Service
    using prompt name and optional version.
    Uses paginated list_prompts API to resolve prompt ID from name.

    Args:
        prompt_name (str): The name of the prompt to retrieve.
        prompt_version (str, optional): Specific version of the prompt.
        Defaults to None.

    Returns:
        tuple: (template_text: str, input_variables: list of str)
    """
    logger.info(
        f"Fetching prompt with name: '{prompt_name}' "
        f"and version: '{prompt_version}'"
    )

    try:
        # Paginate through list_prompts to find the prompt by name
        next_token = None
        matched_prompt = None

        while True:
            params = {}
            if next_token:
                params["nextToken"] = next_token

            response = bedrock_agent.list_prompts(**params)
            for prompt_summary in response.get("promptSummaries", []):
                if prompt_summary["name"] == prompt_name:
                    matched_prompt = prompt_summary
                    break

            if matched_prompt or "nextToken" not in response:
                break

            next_token = response["nextToken"]

        if not matched_prompt:
            raise ValueError(f"No prompt found with name: {prompt_name}")

        prompt_id = matched_prompt["id"]

        # Fetch the full prompt definition using ID and optional version
        if prompt_version:
            prompt = bedrock_agent.get_prompt(
                promptIdentifier=prompt_id, promptVersion=prompt_version
            )
        else:
            prompt = bedrock_agent.get_prompt(promptIdentifier=prompt_id)

        # Extract template and input variables
        text_config = prompt["variants"][0]["templateConfiguration"]["text"]
        template = text_config["text"]
        input_variables = [
            list(var.values())[0] for var in text_config["inputVariables"]
        ]

        return template, input_variables

    except Exception as e:
        logger.error(
            f"Error retrieving prompt '{prompt_name}' "
            f"(version: {prompt_version}): {e}"
        )
        raise
