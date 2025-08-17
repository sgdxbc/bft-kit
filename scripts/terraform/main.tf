terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  owners = ["099720109477"] # Canonical
}

output "ubuntu_ami_name" {
  value = data.aws_ami.ubuntu.name
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "main" {
  vpc_id                  = resource.aws_vpc.main.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = resource.aws_vpc.main.id
}

resource "aws_route_table" "main" {
  vpc_id = resource.aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.main.id
  subnet_id      = resource.aws_subnet.main.id
}

resource "aws_security_group" "main" {
  vpc_id = resource.aws_vpc.main.id

  ingress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_key_pair" "main" {
  public_key = file("~/.ssh/id_rsa.pub")
}

resource "aws_instance" "main" {
  count = 1

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c6a.large"
  subnet_id              = resource.aws_subnet.main.id
  vpc_security_group_ids = [resource.aws_security_group.main.id]
  key_name               = aws_key_pair.main.key_name
}

output "instances" {
  value = aws_instance.main.*
}
