terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "CloudPulse"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "production"
}

# ==========================================
# 1. AWS Networking (VPC & Subnets)
# ==========================================
resource "aws_vpc" "cloudpulse_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "cloudpulse-vpc" }
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.cloudpulse_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
  tags = { Name = "cloudpulse-public-subnet-1" }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.cloudpulse_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true
  tags = { Name = "cloudpulse-public-subnet-2" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.cloudpulse_vpc.id
  tags   = { Name = "cloudpulse-igw" }
}

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.cloudpulse_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "a1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public_rt.id
}

resource "aws_route_table_association" "a2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public_rt.id
}

# ==========================================
# 2. Cloud Messaging & Dead Letter Queue (SQS)
# ==========================================
resource "aws_sqs_queue" "telemetry_dlq" {
  name                      = "cloudpulse-telemetry-dlq"
  message_retention_seconds = 1209600 # 14 days retention for post-mortems
}

resource "aws_sqs_queue" "telemetry_queue" {
  name                      = "cloudpulse-telemetry-queue"
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.telemetry_dlq.arn
    maxReceiveCount     = 3
  })
}

# ==========================================
# 3. Application Load Balancer (ALB)
# ==========================================
resource "aws_security_group" "alb_sg" {
  name        = "cloudpulse-alb-sg"
  description = "Allow inbound HTTP/HTTPS traffic"
  vpc_id      = aws_vpc.cloudpulse_vpc.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "alb" {
  name               = "cloudpulse-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb_sg.id]
  subnets            = [aws_subnet.public_1.id, aws_subnet.public_2.id]
}

resource "aws_lb_target_group" "tg" {
  name        = "cloudpulse-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.cloudpulse_vpc.id
  target_type = "ip"

  health_check {
    path                = "/healthz"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
    matcher             = "200"
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.alb.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.tg.arn
  }
}

# ==========================================
# 4. ECS Fargate Cluster & Service
# ==========================================
resource "aws_ecs_cluster" "cluster" {
  name = "cloudpulse-cluster"
}

resource "aws_cloudwatch_log_group" "ecs_logs" {
  name              = "/ecs/cloudpulse"
  retention_in_days = 14
}

resource "aws_security_group" "ecs_sg" {
  name        = "cloudpulse-ecs-sg"
  description = "Allow incoming traffic from ALB to ECS Fargate"
  vpc_id      = aws_vpc.cloudpulse_vpc.id

  ingress {
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "load_balancer_dns" {
  description = "Public ALB DNS Endpoint for CloudPulse"
  value       = aws_lb.alb.dns_name
}

output "sqs_telemetry_queue_url" {
  description = "SQS Ingestion Queue URL"
  value       = aws_sqs_queue.telemetry_queue.id
}
