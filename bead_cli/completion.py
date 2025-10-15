"""
Shell completion setup command for bead.

Provides shell-specific completion scripts that users can eval or source.
"""

import os
import sys

from argcomplete import shellcode

from .cmdparse import Command


SUPPORTED_SHELLS = ['bash', 'zsh', 'fish', 'tcsh']


class CmdCompletion(Command):
    '''
    Output shell completion setup code.

    Generate shell-specific completion code that can be eval'd or sourced
    to enable tab-completion for bead commands.

    Usage:
        # Auto-detect shell and output completion code
        eval "$(bead completion)"

        # Specify shell explicitly
        eval "$(bead completion bash)"
        eval "$(bead completion zsh)"

        # Save to completion directory (bash example)
        bead completion bash > /etc/bash_completion.d/bead
    '''

    def declare(self, arg):
        arg(
            'shell',
            metavar='SHELL',
            nargs='?',
            choices=SUPPORTED_SHELLS,
            help=f'Shell to generate completion for ({", ".join(SUPPORTED_SHELLS)}). '
                 'Auto-detects from $SHELL if not specified.'
        )

    def run(self, args, env):
        shell = args.shell

        # Auto-detect shell if not specified
        if not shell:
            shell = self._detect_shell()
            if not shell:
                print(
                    "ERROR: Could not auto-detect shell. "
                    f"Please specify one of: {', '.join(SUPPORTED_SHELLS)}",
                    file=sys.stderr
                )
                return 1

        # Generate shell code
        try:
            code = shellcode(['bead'], shell=shell)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1

        # Output instructions as comments + code
        self._print_instructions(shell)
        print(code)
        return 0

    def _detect_shell(self):
        """Detect current shell from environment."""
        shell_path = os.environ.get('SHELL', '')
        if not shell_path:
            return None

        # Extract shell name from path (e.g., /bin/bash -> bash)
        shell_name = os.path.basename(shell_path)

        # Map common shell names
        if shell_name in SUPPORTED_SHELLS:
            return shell_name

        # Handle variations
        if shell_name.startswith('bash'):
            return 'bash'
        elif shell_name.startswith('zsh'):
            return 'zsh'

        return None

    def _print_instructions(self, shell):
        """Print usage instructions as comments."""
        if shell == 'bash':
            print("# Add this to your ~/.bashrc:")
            print("#   eval \"$(bead completion bash)\"")
            print("# Or save to completion directory:")
            print("#   bead completion bash > /etc/bash_completion.d/bead")
            print("#   bead completion bash > ~/.bash_completion.d/bead")
        elif shell == 'zsh':
            print("# Add this to your ~/.zshrc:")
            print("#   eval \"$(bead completion zsh)\"")
            print("# Or save to completion directory:")
            print("#   bead completion zsh > ~/.zsh/completions/_bead")
        elif shell == 'fish':
            print("# Add this to your ~/.config/fish/config.fish:")
            print("#   bead completion fish | source")
            print("# Or save to completion directory:")
            print("#   bead completion fish > ~/.config/fish/completions/bead.fish")
        elif shell == 'tcsh':
            print("# Add this to your ~/.tcshrc:")
            print("#   eval `bead completion tcsh`")
        print()
