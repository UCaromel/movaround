# Contributing to Mov'Around

At this stage the solution needs a complete refactoring, so any help will be welcome :)
Here is a TODO shortlist:

- [x] extraction of features into dedicated R packages/repositories
- [x] separation of concerns between data fetching, presentation and manipulation by the user according to an MVC scheme
- [ ] setup of Github Actions to get CI/CD


## Fixing typos

You can fix typos, spelling mistakes, or grammatical errors in the documentation directly using the GitHub web interface, as long as the changes are made in the source file. This generally means you’ll need to edit roxygen2 comments in an .R, not a .Rd file. You can find the .R file that generates the .Rd by reading the comment in the first line.

## Bigger changes

If you want to make a bigger change, it’s a good idea to first file an issue and make sure someone from the team agrees that it’s needed.

## Bugs

If you’ve found a bug, please file an issue that illustrates the bug with a minimal reprex (this will also help you write a unit test, if needed).


## Pull Requests

### To our new contributors

Please ensure that your pull request (PR) fulfill the following:

- is made from your personal fork of the project
- targets the `develop` branch of the `agistaterre/mov-around` repository
- the whole feature (branch or commit) follows the [commit conventions](#commit-conventions)


### To our new integrators

Please ensure that the PR you are about to merge meets the following requirements:

- code has been reviewed
- commit follows the [conventions](#commit-conventions)


### To the release manager

A release is not required for every merged PR; the release planning is independent of PRs and is scheduled by the release manager.
The steps for a successful release are:

- a release is made of a single, final commit from `develop` beeing merged into `main`
- the commit must be 100% functional and documented
- a tag must be applied to the merge commit
- the tag must follow the [semantic versioning conventions](https://semver.org/)
- no more commits should be required to get the release finalized


## Commit conventions

A commit is:

- minimal
- functional
- standalone
- documented
- tested
- provided with an explicit message
- following the [coding conventions](#coding-conventions)

Commit messages are written in US English and follow the [Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/) structure:

```
type: message

long description if required, leaving a blank line as second line
```

where `message` starts with an action verb at the present tense and `type` should be used according to the following table:

| type | when to use it |
|------|-------------|
| feat | introduction of a new functionality (feature) |
| fix  | introduction of a bug fix |
| doc  | changes in documentation, code comment etc. |
| logging | changes in the program's console outputs |
| test | changes in unit testing (or functional testing) |
| refactor | when functionality remains the same but code has been improved in a manner |
| build | when commit is about the compiling process of R into bytecode |
| packaging | when commit is about creating or changing a R packaging process or content |
| ci | when branching or changing Github Actions for Continuous Integration or Continuous Delivery purposes |
| vcs | for git matters (changes in .gitignore, .gitattributes etc.) |

Some examples are:

```
feat: add a Upload button to upload files
```

```
feat: improve data fetching performance

previous implementation used to call the entire fetching loop every iteration. Now the data sub-domain is preprocessed to fetch the whole data in a single call, saving the superfluous calls overhead.
```

```
refactor: insulate data fetching functions into a dedicated file
```

```
packaging: create a dedicated R package for dataFetcher
```


## Coding conventions

New code should follow the `tidyverse` style guide. You can use the styler package to apply these styles, but please don’t restyle code that has nothing to do with your PR.

We use roxygen2, with Markdown syntax, for documentation.

We use testthat for unit tests. Contributions with test cases included are easier to accept.


## Naming conventions

- functions' names start with verbs
- all names are in English
- all names are meaningful, hence avoid `variable1`, `box1`, etc.
- no uppercase
- the word separator in names is the underscore "_"
- a glossary accurately defines the objects (eg clarify what is a segment vs. a sensor name vs. a sensor ID)



## Dev env

Please read the appropriate chapters in the [README.md](README.md) file to set up an appropriate development environment.


### Tests

Please read the appropriate chapters in the [README.md](README.md) file.


### License

By contributing to Mov'Around, you agree that your contributions will be
licensed under the terms given in the [LICENSE](LICENSE) file.
