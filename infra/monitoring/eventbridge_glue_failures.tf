# SNS topic for alerts
resource "aws_sns_topic" "glue_job_alerts" {
  name = "glue-job-failure-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.glue_job_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# EventBridge rule: catches Glue job FAILED/TIMEOUT/STOPPED states
resource "aws_cloudwatch_event_rule" "glue_job_failure" {
  name        = "glue-job-failure-rule"
  description = "Triggers when a Glue job run fails"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      state = ["FAILED", "TIMEOUT", "STOPPED"]
      # optionally scope to specific jobs:
      # jobName = ["bronze_ingestion"]
    }
  })
}

resource "aws_cloudwatch_event_target" "sns_target" {
  rule      = aws_cloudwatch_event_rule.glue_job_failure.name
  target_id = "send-to-sns"
  arn       = aws_sns_topic.glue_job_alerts.arn
}

# Allow EventBridge to publish to SNS
resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn = aws_sns_topic.glue_job_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgePublish"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "SNS:Publish"
      Resource  = aws_sns_topic.glue_job_alerts.arn
    }]
  })
}
