resource "aws_sqs_queue" "sqs_queue" {
  name = var.name
}