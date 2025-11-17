VERSION=v1.0.2

release:
	echo "Only create it after you push the changes to the repository # master"
	go mod tidy; git add .; git commit -m "Release $(VERSION)"; git push origin master; git tag $(VERSION); git push origin $(VERSION);
