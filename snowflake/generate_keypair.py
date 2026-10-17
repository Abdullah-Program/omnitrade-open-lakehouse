"""
================================================================================
RSA Key-Pair Generator for Snowflake Automation (Zero MFA Bypass)
================================================================================
Role: Principal AWS Data Engineer
Layer: Enterprise Security & CI/CD Credentials
Description:
    Generates a 2048-bit RSA PKCS#8 key pair for Snowflake service authentication.
    Bypasses interactive MFA prompts for dbt, Airflow, and automated pipelines.
================================================================================
"""

import os
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

def generate_snowflake_keypair():
    print("\n" + "=" * 80)
    print("      GENERATING RSA 2048-BIT KEY-PAIR FOR SNOWFLAKE")
    print("=" * 80)

    # 1. Generate 2048-bit Private Key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # 2. Save Private Key (PKCS#8 unencrypted for local dbt development)
    key_dir = os.path.abspath(".")
    private_key_file = os.path.join(key_dir, "rsa_key.p8")
    
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    with open(private_key_file, "wb") as f:
        f.write(private_pem)

    print(f"1. Private Key saved locally to: {private_key_file}")

    # 3. Extract Public Key in Snowflake Format
    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    # Extract base64 payload without header/footer lines
    pub_lines = [l.strip() for l in public_pem.decode("utf-8").splitlines() if not l.startswith("---")]
    public_key_clean = "".join(pub_lines)

    print("\n2. COPY & RUN THIS EXACT SQL IN YOUR SNOWFLAKE WORKSHEET (Browser Tab):")
    print("-" * 80)
    sql_cmd = f"ALTER USER HONEY666 SET RSA_PUBLIC_KEY='{public_key_clean}';"
    print(sql_cmd)
    print("-" * 80)
    print("Once executed in Snowflake, dbt will authenticate with zero MFA prompts!\n")

    return private_key_file, sql_cmd

if __name__ == "__main__":
    generate_snowflake_keypair()
