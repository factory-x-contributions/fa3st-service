#!/usr/bin/env bash
#
VERSION=$1

TAG_VERSION="version"
TAG_DOWNLOAD_RELEASE="download-release"
TAG_DOWNLOAD_SNAPSHOT="download-snapshot"
TAG_CHANGELOG_HEADER="changelog-header"
CHANGELOG_FILE="./docs/source/other/release-notes.md"
README_FILE="README.md"
INSTALLATION_FILE="./docs/source/basics/installation.md"
README_LATEST_RELEASE_VERSION_CONTENT="[Download latest RELEASE version \($VERSION\)]\(not published on maven repositories\)"
INSTALLATION_LATEST_RELEASE_VERSION_CONTENT="{download}\`Latest RELEASE version \($VERSION\) \(not published on maven repositories\)\`"

# arguments: tag
function startTag()
{
	echo "<!--start:$1-->\\n"
}

# arguments: tag
function endTag()
{
	echo "<!--end:$1-->"
}

# arguments: file, tag, newValue, originalValue(optional, default: matches anything)
function replaceValue()
{
	local file=$1
	local tag=$2
	local newValue=$3
	local originalValue=${4:-.*}
	local startTag=$(startTag "$tag")
	local endTag=$(endTag "$tag")
	sed -r -z "s/$startTag($originalValue)$endTag/$startTag$newValue$endTag/g" -i "$file"
}

# arguments: file, tag
function removeTag()
{
	local file=$1
	local tag=$2
	local startTag=$(startTag "$tag")
	local endTag=$(endTag "$tag")
	sed -r -z "s/$startTag(.*)$endTag/\1/g" -i "$file"
}

# arguments: file, newVersion
function replaceVersion()
{
	local file=$1
	local new_version=$2
	local startTag=$(startTag "$tag")
	local endTag=$(endTag "$tag")
	replaceValue "$file" "$TAG_VERSION" "$new_version"
	sed -r -z 's/(<artifactId>starter<\/artifactId>[\r\n]+\s*<version>)[^<]+(<\/version>)/\1'"${new_version}"'\2/g' -i "$file"
	sed -r -z 's/(\x27de.fraunhofer.iosb.ilt.faaast.service:starter:)[^\x27]*\x27/\1'"${new_version}"'\x27/g' -i "$file"
}

echo "Releasing:  ${VERSION},
tagged:    v${VERSION}"

echo "Replacing version numbers"
sed -i 's/<tag>HEAD<\/tag>/<tag>v'"${VERSION}"'<\/tag>/g' pom.xml
replaceVersion "$README_FILE" "$VERSION"
replaceValue "$README_FILE" "$TAG_DOWNLOAD_SNAPSHOT" ""
replaceValue "$README_FILE" "$TAG_DOWNLOAD_RELEASE" "$README_LATEST_RELEASE_VERSION_CONTENT"
replaceVersion "$INSTALLATION_FILE" "$VERSION"
replaceValue "$INSTALLATION_FILE" "$TAG_DOWNLOAD_SNAPSHOT" ""
replaceValue "$INSTALLATION_FILE" "$TAG_DOWNLOAD_RELEASE" "$INSTALLATION_LATEST_RELEASE_VERSION_CONTENT"
replaceValue "$CHANGELOG_FILE" "$TAG_CHANGELOG_HEADER" "## ${VERSION}"
removeTag "$CHANGELOG_FILE" "$TAG_CHANGELOG_HEADER"

echo "build project (no tests) to apply version changes"
mvn clean install -DskipTests

echo "Apply maven spotless"
mvn -B spotless:apply

echo "Updating third party license report" # TODO dont skip test, spotless
mvn clean install license:aggregate-third-party-report -P build-ci -B

echo "Done"