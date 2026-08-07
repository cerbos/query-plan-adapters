/** Stop the container `jest.globalSetup.adversarial.js` started, if there was one. */
module.exports = async function globalTeardown() {
  const container = globalThis.__ADVERSARIAL_POSTGRES__;
  if (container) {
    await container.stop();
    globalThis.__ADVERSARIAL_POSTGRES__ = undefined;
  }
};
