# Load EventBridge rules configuration
locals {
  eventbridge_config = yamldecode(file("${path.module}/../config/eventbridge-rules/${var.env_suffix}/rules.yaml"))
  eventbridge_rules  = local.eventbridge_config.eventbridge_rules
  scheduled_rules    = local.eventbridge_config.scheduled_rules

  # Extract unique step function ARNs for validation (only for enabled rules)
  step_function_arns = toset([
    for rule in local.eventbridge_rules :
    rule.source_step_function_arn
    if rule.rule_type == "step_function_completion" && try(rule.enabled, false)
  ])

  # Event pattern templates
  event_patterns = {
    s3_object_created = {
      source      = ["aws.s3"]
      detail-type = ["Object Created"]
    }
    step_function_completion = {
      source      = ["aws.states"]
      detail-type = ["Step Functions Execution Status Change"]
    }
  }

  # Generate descriptions for each rule
  rule_descriptions = {
    for rule in local.eventbridge_rules : rule.name =>
    rule.rule_type == "s3_object_created" ?
    "Trigger ${rule.target_step_function} when S3 file is created in ${rule.s3_bucket_name}" :
    "Trigger ${rule.target_step_function} when step function completes successfully"
  }

  # Generate event patterns for each rule
  rule_event_patterns = {
    for rule in local.eventbridge_rules : rule.name =>
    rule.rule_type == "s3_object_created" ? merge(local.event_patterns.s3_object_created, {
      detail = {
        bucket = {
          name = [rule.s3_bucket_name]
        }
        object = {
          key = [{
            prefix = rule.s3_key_prefix
          }]
        }
      }
      }) : merge(local.event_patterns.step_function_completion, {
      detail = {
        status          = [rule.completion_status]
        stateMachineArn = [rule.source_step_function_arn]
      }
    })
  }
}

# Validate that source step functions exist (only if they are enabled)
# Comment out this block if source Step Functions don't exist yet
# data "aws_sfn_state_machine" "source_step_functions" {
#   for_each = local.step_function_arns
#   name     = split(":", each.value)[6] # Extract name from ARN
# }

# EventBridge rules from config
resource "aws_cloudwatch_event_rule" "aws_samples_udm_triggers" {
  for_each = { for rule in local.eventbridge_rules : rule.name => rule }

  name        = "aws-samples-udm-${each.value.name}-${var.env_suffix}"
  description = local.rule_descriptions[each.key]
  state       = try(each.value.enabled, true) ? "ENABLED" : "DISABLED"

  event_pattern = jsonencode(local.rule_event_patterns[each.key])
}

# EventBridge targets from config
resource "aws_cloudwatch_event_target" "aws_samples_udm_step_function_targets" {
  for_each = { for rule in local.eventbridge_rules : rule.name => rule }

  rule      = aws_cloudwatch_event_rule.aws_samples_udm_triggers[each.key].name
  target_id = "AwsSamplesUDM${title(each.value.name)}Target"
  arn       = aws_sfn_state_machine.aws_samples_udm_pipeline.arn
  role_arn  = aws_iam_role.aws_samples_udm_eventbridge_role.arn

  input = jsonencode({
    for k, v in each.value.input_parameters :
    k => k == "tables_list" ? [
      for table in v :
      replace(table, "<<env>>", var.env_suffix)
    ] : v
  })
}

# IAM role for EventBridge to invoke Step Function
resource "aws_iam_role" "aws_samples_udm_eventbridge_role" {
  name = "aws-samples-udm-eventbridge-role-${var.env_suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for EventBridge to invoke Step Functions
resource "aws_iam_role_policy" "aws_samples_udm_eventbridge_policy" {
  name = "aws-samples-udm-eventbridge-policy-${var.env_suffix}"
  role = aws_iam_role.aws_samples_udm_eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.aws_samples_udm_pipeline.arn
      }
    ]
  })
}

# Scheduled EventBridge rules from config
resource "aws_cloudwatch_event_rule" "aws_samples_udm_scheduled_triggers" {
  for_each = { for rule in local.scheduled_rules : rule.name => rule }

  name                = "aws-samples-${replace(each.value.name, "_", "-")}-${var.env_suffix}"
  description         = "Scheduled trigger for ${each.value.target_step_function}"
  schedule_expression = each.value.schedule_expression
  state               = try(each.value.enabled, true) ? "ENABLED" : "DISABLED"
}

# EventBridge targets for scheduled Step Function executions
resource "aws_cloudwatch_event_target" "aws_samples_udm_scheduled_targets" {
  for_each = { for rule in local.scheduled_rules : rule.name => rule }

  rule      = aws_cloudwatch_event_rule.aws_samples_udm_scheduled_triggers[each.key].name
  target_id = "AwsSamplesUDM${title(replace(each.value.name, "_", "-"))}Target"
  arn       = aws_sfn_state_machine.aws_samples_udm_pipeline.arn
  role_arn  = aws_iam_role.aws_samples_udm_eventbridge_role.arn

  input = jsonencode({
    tables_list = [
      for table in each.value.tables_list :
      replace(table, "<<env>>", var.env_suffix)
    ]
  })
}