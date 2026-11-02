export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    // This repo's history writes subjects in sentence case ("fix: Keep ..."),
    // which config-conventional forbids by default. The rest of its list stays.
    'subject-case': [2, 'never', ['start-case', 'pascal-case', 'upper-case']],
  },
}
