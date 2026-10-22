"""Fetch the two S3A dependencies matched to Spark 3.5.6 / Hadoop 3.3.4."""
import argparse
import hashlib
from pathlib import Path
import urllib.request


ARTIFACTS = {
    "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar":
        "53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a",
    "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar":
        "873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="/opt/spark/jars")
    args = parser.parse_args()
    directory = Path(args.directory)
    if not (directory / "hadoop-client-runtime-3.3.4.jar").exists():
        raise ValueError("This image does not contain Hadoop 3.3.4; do not mix S3A dependency versions")
    for path, expected in ARTIFACTS.items():
        target = directory / path.rsplit("/", 1)[-1]
        temporary = target.with_suffix(".download")
        checksum = hashlib.sha256()
        with urllib.request.urlopen("https://repo.maven.apache.org/maven2/" + path, timeout=60) as response, temporary.open("wb") as out:
            while chunk := response.read(1024 * 1024):
                checksum.update(chunk)
                out.write(chunk)
        if checksum.hexdigest() != expected:
            temporary.unlink()
            raise ValueError(f"Artifact SHA256 mismatch: {target.name}")
        temporary.replace(target)
        print(f"Verified {target.name}", flush=True)


if __name__ == "__main__":
    main()
