Given:

- distributed group of people with little time to spend on a project (think: < 8 hours a week)
- a tool that can package data, code and dependency information, data (and code used to create it) is shared as .zip files created with the tool


---

People: Alice, Bob, Lia, Jack


---


USE CASE:  Jack joins the team and requests new data similar to one of the existing packages.
(Problem: names are not perfect locally, there is a disagreement on names globally)

---

Alice cleans data, that will be used as base data, she gives names, knowing that names are important hints.

Bob is a funny guy, who uses surprising names in his code and uses cryptic names for others, but he is able to deliver interesting results.

Lia is an analyst working with data from Alice and Bob, however for her sanity she renames everything from Bob for her own use.

Jack is another analyst arriving late in the project, knowing little about existing data.


---

Jack needs data for his analysis, and asks Lia for it.

Lia describes the data available (with her naming) and also sends links to data to Jack via email/IRC/chat/Slack.

Jack creates a new workspace for a new product and adds the files from Lia as inputs.

After Lia going offline for a week Jack realizes, that he needs a slightly different version of Bob's file.
Fortunately Bob is online and is ready to work.
Jack needs to tells Bob what package needs a modification and how, but Lia warned Jack, that Bob names his stuff differently.

Jack asks Bob for his naming for packages.
Bob *exports* his naming with the tool and sends the exported file to Jack via Slack.
Jack *imports* the file from Bob with the tool.
Jack *queries the status* of his workspace with an option to show all known names, which now included Bob's names as well.
Jack tells Bob what is the package name (using Bob's name of it), and how to modify.

Bob *branches* the package Jack named (create a new workspace with code and dependency information, but with a new identifier, name) and changes the code according to the request.
When the new package is ready, Bob delivers it to Jack via Dropbox, who can now start working on his analysis.



---



USE CASE:  Full package dependency in partially cooperative team.
(Problem: traceability of packages)

---

The company has a policy of full traceability (prerequisite of reproducability): every product should be traceable to some base data (either self collected or received from outside).

This is thought to be achieved by setting up a repository for all packages and every package creator is expected to upload the packages and its known dependencies that are released.  Here release has a very strict meaning: leaving the creators' hand.  So release includes giving a work-in-progress version to a collegue for quick review.

Alice releases package for Bob who releases package to Lia.
Lia releases package for outside use.
(release means transferring a zip file by any means, USB disk, network, file sharing service)

Alice and Lia uploads their released packages and the dependencies of them to the company repo, but Bob do not cooperate.

The company repo now contains a set of packages that contain even Bob's contribution, as Lia has uploaded it as a dependency of her packages.

(the precondition of full traceability is that Bob does not have any intermediate unreleased packages, as they were private to Bob!)

Upload is also a repository specific procedure, FTP, S3, Dropbox, rsync, Windows share, or any other service for storing files.
