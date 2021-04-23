const core = require('@actions/core');
const github = require('@actions/github');

const trustedReviewers = [
  'sileht',
  'jd',
];
const trustedAuthors = [
  ...trustedReviewers,
  'dependabot[bot]',
];

const syncer = async () => {
  try {
    const token = core.getInput('token');
    const commitsToValidate = parseInt(core.getInput('commits-to-validate'), 10);
    const octokit = github.getOctokit(token);

    const repoProd = {
      owner: 'Mergifyio',
      repo: 'mergify-engine-prod',
    };

    const repoEngine = {
      owner: 'Mergifyio',
      repo: 'mergify-engine',
    };

    const { data: submoduleFile } = await octokit.rest.repos.getContent({
      ...repoProd,
      path: 'mergify-engine',
    });

    if (submoduleFile.type !== 'submodule') {
      throw new Error(`${submoduleFile.path} is not a submodule`);
    }

    const currentEngineProdSha = submoduleFile.sha;

    const { data: commits } = await octokit.rest.repos.listCommits({
      ...repoEngine,
      per_page: commitsToValidate,
    });

    const idx = commits.findIndex((c) => c.sha === currentEngineProdSha);
    if (idx < 0) {
      throw new Error('current production engine sha not found in last ');
    }
    const deltaCommits = commits.slice(0, idx);

    const latestEngineSha = commits[0].sha;

    core.setOutput('current-engine-sha', currentEngineProdSha);
    core.setOutput('latest-engine-sha', latestEngineSha);
    core.setOutput('commits-to-validate', deltaCommits.length);

    if (idx === 0) {
      console.log('Already up2date');
      return;
    }

    let body = '';
    for (const commit of deltaCommits) {
      // Behave this API may returns pulls request from other forked repositories
      let { data: pulls } = await octokit.rest.repos.listPullRequestsAssociatedWithCommit({
        ...repoEngine,
        commit_sha: commit.sha,
      });
      pulls = pulls.filter((p) => (p.state === 'closed' && p.base.ref === 'master' && p.base.user.login === 'Mergifyio'));
      if (pulls.length > 1) {
        core.setOutput('untrusted-commit-sha', commit.sha);
        throw new Error(`Multiple pull request associated with ${commit.sha}`);
      } else if (pulls.length < 1) {
        core.setOutput('untrusted-commit-sha', commit.sha);
        throw new Error(`No pull request associated with ${commit.sha}`);
      }
      const pull = pulls[0];
      const { data: reviews } = await octokit.rest.pulls.listReviews({
        ...repoEngine,
        pull_number: pull.number,
      });

      const allowedApprovals = new Map();
      // Reviews are by chronological order, so we need to keep only the last one
      for (const review of reviews) {
        if (trustedReviewers.includes(review.user.login)) {
          if (review.state === 'APPROVED') {
            allowedApprovals.set(review.user.login, review);
          } else {
            allowedApprovals.delete(review.user.login);
          }
        }
      }

      const reviewers = Array.from(allowedApprovals.keys()).join(',');
      const authorTrusted = trustedAuthors.includes(pull.user.login);
      const reviewersTrusted = allowedApprovals.size > 0;
      const message = `commit: ${commit.sha}, pull: ${pull.html_url}, author: ${pull.user.login}, reviewers: ${reviewers || '<none>'}, authorTrusted: ${authorTrusted}, reviewersTrusted: ${reviewersTrusted}`;
      console.log(message);
      body += `${pull.html_url} (${commit.sha.slice(0, 7)}): ${pull.user.login} | ${pull.title} | reviewed by: ${reviewers || '<none>'}\n`;
      if (!authorTrusted && !reviewersTrusted) {
        core.setOutput('untrusted-commit-sha', commit.sha);
        core.setOutput('untrusted-author', pull.user.login);
        core.setOutput('untrusted-reviewers', reviewers);
        core.setOutput('untrusted-pull-request', pull.html_url);
        throw new Error(`Untrusted commit found: ${commit.sha}`);
      }
    }

    const { data: branchMainProd } = await octokit.rest.repos.getBranch({
      ...repoProd,
      branch: 'main',
    });
    const { data: currentTree } = await octokit.rest.git.getTree({
      ...repoProd,
      tree_sha: branchMainProd.commit.sha,
    });

    const { data: newTree } = await octokit.rest.git.createTree({
      ...repoProd,
      base_tree: currentTree.sha,
      tree: [{
        path: 'mergify-engine',
        mode: '160000', // submodule mode
        type: 'commit',
        sha: latestEngineSha,
      }],
    });

    const { data: newCommit } = await octokit.rest.git.createCommit({
      ...repoProd,
      message: `feat: Bump mergify-engine from ${currentEngineProdSha.slice(0, 7)} to ${latestEngineSha.slice(0, 7)}\n\n${body}`,
      tree: newTree.sha,
      parents: [branchMainProd.commit.sha],
    });

    await octokit.rest.git.updateRef({
      ...repoProd,
      ref: 'heads/main',
      sha: newCommit.sha,
    });

    console.log(`Main branch have been updated from ${branchMainProd.commit.sha} to ${newCommit.sha}`);
  } catch (error) {
    console.log(error);
    core.setFailed(error.message);
  }
};

syncer();
