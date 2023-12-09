# Apache Spark and Hadoop Installation on Windows

## Step 1: Apache Spark Installation

1. **Download Apache Spark:** [Apache Spark Downloads](https://spark.apache.org/downloads.html)

2. **Extract the downloaded file:** Extract the contents to your preferred location.

3. **Set SPARK_HOME environment variable:**
   - Open a command prompt.
   - Run the following command, replacing `<SparkInstallationPath>` with the path where you extracted Spark.
     ```bash
     setx SPARK_HOME "<SparkInstallationPath>"
     ```
     Example:
     ```bash
     setx SPARK_HOME "C:\spark-3.2.0-bin-hadoop3.2"
     ```
   - Close and reopen the command prompt.

4. **Add Spark bin directory to PATH:**
   - Append `%SPARK_HOME%\bin` to your `PATH` environment variable.
     ```bash
     setx PATH "%PATH%;%SPARK_HOME%\bin"
     ```
   - Close and reopen the command prompt.

## Step 2: Hadoop Installation

1. **Download Hadoop for Windows:** [Apache Hadoop Releases](https://hadoop.apache.org/releases.html)

2. **Extract the downloaded file:**
   - Extract the contents to a directory of your choice.

3. **Set HADOOP_HOME environment variable:**
   - Open a command prompt.
   - Run the following command, replacing `<HadoopInstallationPath>` with the path where you extracted Hadoop.
     ```bash
     setx HADOOP_HOME "<HadoopInstallationPath>"
     ```
     Example:
     ```bash
     setx HADOOP_HOME "C:\hadoop-3.3.0"
     ```
   - Close and reopen the command prompt.

---

## Step 3: Installing AWS CLI on Windows

1. Download the AWS CLI installer for Windows: [AWS CLI Installer for Windows](https://aws.amazon.com/cli/)

2. Run the installer and follow the instructions to complete the installation.

3. After installation, open a new command prompt and run:
   ```bash
   aws --version
This should display the installed AWS CLI version.

## Step 4: Setting up a Cluster on AWS

### Setup of EMR on AWS

1. **Navigate to EMR in AWS Console:**
   - Go to AWS Management Console.
   - Navigate to the Amazon EMR service.

2. **Create Cluster:**
   - Click on "Create Cluster."
   - Provide a name for your cluster.

3. **Select EMR Release:**
   - Choose the appropriate Amazon EMR release, e.g., emr-6.15.0.

4. **Configure Instances:**
   - Click on "Cluster Configuration."
   - Add an instance group to scale your cluster.
   - Configure the instance types and the number of instances.
   - Click on "Add Instance Group" and then "Add."

5. **Security Configuration:**
   - Configure security settings and EC2 key pair.
   - Add an Amazon EC2 key pair for SSH access to the cluster.

6. **Select Roles:**
   - Configure roles:
     - Amazon EMR service role: EMR_DefaultRole.
     - EC2 instance profile for Amazon EMR: EMR_DefaultRole.

7. **Create Cluster:**
   - Click on "Create Cluster."
   - After the cluster is created, go to the security settings of EC2 instances and open port 22 for SSH.

8. **Connect to EMR Instance (Windows):**
   - Connect to SSH Server using an SSH client like PuTTY.
   - Copy files to EMR Instance using `scp`.
   - Reconnect to the server using the SSH command.
   - Execute Commands in a Virtual Environment.

## Step 5: Setting up Prediction on EC2 Instance

### Setup of EC2 Instance on AWS

1. **Launch an EC2 Instance:**
   - Go to the AWS Management Console.
   - Navigate to the EC2 service.
   - Click on "Launch Instance."

2. **Connect with SSH Command:**
   - After creating the instance, use an SSH client like PuTTY for Windows to connect.

3. **Install Java:**
   - Install OpenJDK using the command prompt on Windows.
   - Add the Java installation path to the environment variables.

4. **Install AWS CLI:**
   - Install AWS CLI using a package manager like chocolatey on Windows.
   - Configure AWS CLI using the `aws configure` command.

5. **Install Hadoop:**
   - Download and Extract Hadoop.
   - Configure Hadoop and add the installation path to the environment variables.
   - Test Hadoop Installation.

6. **Install Spark:**
   - Download and Extract Spark.
   - Configure Spark and add the installation path to the environment variables.
   - Test Spark Installation.

7. **Setup Python in AWS EC2 (Windows):**
   - Download and install the latest version of Python from python.org.
   - Install `virtualenv`.
   - Create a virtual environment and activate it.
   - Install project dependencies from `requirements.txt`.
   - Execute the command using `spark-submit`.

---