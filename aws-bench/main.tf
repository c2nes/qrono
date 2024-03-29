terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }

    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = "~> 2.1"
    }
  }
}

provider "aws" {
  profile = "default"
  region  = "us-east-1"
}

variable "additional_tags" {
  type = map(string)
  default = {
    Application = "qrono-bench"
  }
}

variable "public_key_path" {
  type    = string
  default = "~/.ssh/id_rsa.pub"
}

# Instance type must be supported by Cluster Placement Group.
# Burstable instance types (T2, T3, etc.) in particular are not supported.
# See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html#concepts-placement-groups
variable "client_instance_type" {
  type    = string
  default = "c6a.8xlarge"
}
variable "server_instance_type" {
  type    = string
  default = "im4gn.16xlarge"
}

variable "availability_zone" {
  type    = string
  default = "us-east-1b"
}

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  tags = merge(
    var.additional_tags,
    {
      Name = "qrono-bench/main"
    }
  )
}

resource "aws_subnet" "main" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.10.0/24"
  map_public_ip_on_launch = true
  availability_zone       = var.availability_zone

  tags = merge(
    var.additional_tags,
    {
      Name = "main"
    }
  )
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id

  tags = merge(
    var.additional_tags,
    {
      Name = "gw"
    }
  )
}

resource "aws_route_table" "default" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }

  tags = merge(
    var.additional_tags,
    {
      Name = "default"
    }
  )
}

resource "aws_main_route_table_association" "main" {
  vpc_id         = aws_vpc.main.id
  route_table_id = aws_route_table.default.id
}

resource "aws_security_group" "default" {
  vpc_id = aws_vpc.main.id

  ingress {
    protocol  = "-1"
    from_port = 0
    to_port   = 0
    self      = true
  }

  ingress {
    protocol    = "tcp"
    from_port   = 22
    to_port     = 22
    cidr_blocks = ["0.0.0.0/0"]
  }

  # xrdp
  ingress {
    protocol    = "tcp"
    from_port   = 3389
    to_port     = 3389
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    var.additional_tags,
    {
      Name = "default"
    }
  )
}

resource "aws_key_pair" "default" {
  key_name_prefix = "qrono-bench-"
  public_key      = file(pathexpand(var.public_key_path))
  tags            = var.additional_tags
}

data "aws_ec2_instance_type" "client" {
  instance_type = var.client_instance_type
}

data "aws_ec2_instance_type" "server" {
  instance_type = var.server_instance_type
}

data "aws_ami" "client" {
  owners      = ["099720109477"] # ubuntu
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-impish-21.10-*"]
  }

  filter {
    name   = "architecture"
    values = data.aws_ec2_instance_type.client.supported_architectures
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
}

data "aws_ami" "server" {
  owners      = ["099720109477"] # ubuntu
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-impish-21.10-*"]
  }

  filter {
    name   = "architecture"
    values = data.aws_ec2_instance_type.server.supported_architectures
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
}

resource "aws_placement_group" "benchmark" {
  name     = "qrono-benchmark"
  strategy = "cluster"
}

data "cloudinit_config" "server" {
  gzip          = true
  base64_encode = true

  part {
    content_type = "text/cloud-config"
    content = templatefile(
      "${path.module}/server.init.cfg",
      {
        root = path.module
      }
    )
    filename = "server.init.cfg"
  }

  part {
    content_type = "text/x-shellscript"
    content      = file("${path.module}/server.init.sh")
    filename     = "server.init.sh"
  }
}

resource "aws_instance" "server" {
  ami                         = data.aws_ami.server.id
  instance_type               = var.server_instance_type
  associate_public_ip_address = true
  subnet_id                   = aws_subnet.main.id
  key_name                    = aws_key_pair.default.id
  placement_group             = aws_placement_group.benchmark.id

  user_data_base64 = data.cloudinit_config.server.rendered

  vpc_security_group_ids = [aws_security_group.default.id]

  # ebs_block_device {
  #   device_name           = "/dev/sdf"
  #   volume_type           = "io2"
  #   iops                  = 32000
  #   delete_on_termination = true
  #   volume_size           = 100 # GB
  # }

  volume_tags = var.additional_tags
  tags = merge(
    var.additional_tags,
    {
      Name = "server"
    }
  )
}

data "cloudinit_config" "client" {
  gzip          = true
  base64_encode = true

  part {
    content_type = "text/cloud-config"
    content = templatefile(
      "${path.module}/client.init.cfg.tpl",
      {
        root      = path.module
        server_ip = aws_instance.server.private_ip
      }
    )
    filename = "server.init.cfg"
  }

  part {
    content_type = "text/x-shellscript"
    content      = file("${path.module}/client.init.sh")
    filename     = "server.init.sh"
  }
}

resource "aws_instance" "client" {
  ami                         = data.aws_ami.client.id
  instance_type               = var.client_instance_type
  associate_public_ip_address = true
  subnet_id                   = aws_subnet.main.id
  key_name                    = aws_key_pair.default.id
  placement_group             = aws_placement_group.benchmark.id

  user_data_base64 = data.cloudinit_config.client.rendered

  vpc_security_group_ids = [aws_security_group.default.id]

  tags = merge(
    var.additional_tags,
    {
      Name = "client"
    }
  )
}

output "server_ip" {
  value = aws_instance.server.public_ip
}

output "client_ip" {
  value = aws_instance.client.public_ip
}
